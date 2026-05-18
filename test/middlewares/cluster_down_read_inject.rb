# frozen_string_literal: true

module Middlewares
  # Simulates CLUSTERDOWN during pipelining by omitting matching commands from the
  # wire payload in +write_multi+ and returning a CommandError from +read+ at the
  # same pipeline index (keeps RESP read count in sync).
  module ClusterDownReadInject
    CLUSTERDOWN_RESPONSE = 'CLUSTERDOWN Hash slot not served'

    module Injected
      def write_multi(commands)
        state = cluster_down_inject_state
        unless state
          return super
        end

        state[:indices] = []
        wire_commands = []
        commands.each_with_index do |cmd, index|
          if ClusterDownReadInject.command_matches?(cmd, state[:command])
            state[:indices] << index
          else
            wire_commands << cmd
          end
        end
        state[:position] = 0

        super(wire_commands) unless wire_commands.empty?
        nil
      end

      def read(timeout = nil)
        state = cluster_down_inject_state
        return super(timeout) unless state

        state[:position] += 1
        if state[:indices].include?(state[:position] - 1)
          ::RedisClient::CommandError.new(CLUSTERDOWN_RESPONSE)
        else
          super(timeout)
        end
      end

      private

      def cluster_down_inject_state
        command = Thread.current[:cluster_down_fake_command]
        return nil if command.nil?

        registry = Thread.current[:cluster_down_read_inject] ||= {}
        registry[object_id] ||= { command: command, indices: [], position: 0 }
      end
    end

    module_function

    def install!
      return if @installed

      ::RedisClient::RubyConnection.prepend(Injected)
      @installed = true
    end

    def with(command)
      install!
      Thread.current[:cluster_down_fake_command] = command
      Thread.current[:cluster_down_read_inject] = {}
      yield
    ensure
      Thread.current[:cluster_down_fake_command] = nil
      Thread.current[:cluster_down_read_inject] = nil
    end

    def command_matches?(cmd, command)
      cmd == command || (cmd[0].to_s == command[0] && cmd[1].to_s == command[1])
    end
  end
end
