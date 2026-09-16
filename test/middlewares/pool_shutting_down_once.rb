# frozen_string_literal: true

module Middlewares
  module PoolShuttingDownOnce
    # redis-client sends connection_prelude through call_pipelined on connect.
    PRELUDE_COMMANDS = %w[HELLO AUTH SELECT CLIENT ROLE READONLY].freeze

    class Trigger
      def initialize
        @fired = false
        @mutex = Mutex.new
      end

      def fire!
        @mutex.synchronize do
          return false if @fired

          @fired = true
          true
        end
      end

      def fired?
        @mutex.synchronize { @fired }
      end
    end

    def call_pipelined(commands, redis_config)
      unless prelude_commands?(commands)
        raise ::ConnectionPool::PoolShuttingDownError if redis_config.custom.fetch(:pool_shutting_down_once).fire!
      end

      super
    end

    def prelude_commands?(commands)
      commands.all? { |command| PRELUDE_COMMANDS.include?(command.first.to_s.upcase) }
    end
  end
end
