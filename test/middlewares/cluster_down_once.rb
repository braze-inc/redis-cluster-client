# frozen_string_literal: true

module Middlewares
  module ClusterDownOnce
    PRELUDE_COMMANDS = %w[HELLO AUTH SELECT CLIENT ROLE READONLY].freeze
    CLUSTERDOWN_MESSAGE = 'CLUSTERDOWN Hash slot not served'

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

      def reset!
        @mutex.synchronize { @fired = false }
      end
    end

    def call(command, redis_config)
      unless prelude_command?(command)
        raise ::RedisClient::CommandError, CLUSTERDOWN_MESSAGE if redis_config.custom.fetch(:cluster_down_once).fire!
      end

      super
    end

    def prelude_command?(command)
      PRELUDE_COMMANDS.include?(command.first.to_s.upcase)
    end
  end
end
