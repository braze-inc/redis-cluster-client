# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class Cluster
    class TestPipelinePoolShuttingDown < TestingWrapper
      def setup
        @trigger = ::Middlewares::PoolShuttingDownOnce::Trigger.new
        @captured_commands = ::Middlewares::CommandCapture::CommandBuffer.new
        @redirect_count = ::Middlewares::RedirectCount::Counter.new
        @reload_attempts = []
        @client = build_client
        @client.call('FLUSHDB')
        wait_for_replication
        @captured_commands.clear
        @redirect_count.clear
        @reload_attempts.clear
      end

      def teardown
        @client&.call('FLUSHDB')
        wait_for_replication
        @client&.close
      end

      def test_pooled_pipeline_recovers_from_pool_shutting_down
        key = 'pool_shutdown:test'
        @client.call('SET', key, 'expected')
        wait_for_replication
        @captured_commands.clear

        got = @client.pipelined { |pi| pi.call('GET', key) }

        assert_equal(['expected'], got)
        assert_equal(1, @reload_attempts.size, 'recovery should retry exactly once via try_reload!')
        assert(@trigger.fired?, 'middleware should have fired exactly once')
        redirects = @redirect_count.get
        assert_operator(
          redirects.moved + redirects.ask,
          :<=,
          1,
          "at most one redirection retry, got: #{redirects.inspect}"
        )
      end

      private

      def build_client
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: [
            ::Middlewares::CommandCapture,
            ::Middlewares::PoolShuttingDownOnce,
            ::Middlewares::RedirectCount
          ],
          custom: {
            captured_commands: @captured_commands,
            pool_shutting_down_once: @trigger,
            redirect_count: @redirect_count
          },
          **TEST_GENERIC_OPTIONS
        )
        client = ::RedisClient::Cluster.new(config, pool: { timeout: TEST_TIMEOUT_SEC, size: 2 })
        instrument_try_reload!(client)
        client
      end

      def instrument_try_reload!(client)
        node = client.send(:router).instance_variable_get(:@node)
        original_try_reload = node.method(:try_reload!)
        reload_attempts = @reload_attempts
        node.define_singleton_method(:try_reload!) do |**kwargs|
          reload_attempts << true
          original_try_reload.call(**kwargs)
        end
      end

      def wait_for_replication
        client_side_timeout = TEST_TIMEOUT_SEC + 1.0
        server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
        swap_timeout(@client, timeout: 0.1) do |client|
          client&.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
        rescue ::RedisClient::Cluster::ErrorCollection => e
          raise unless e.errors.values.all? { |err| err.is_a?(::RedisClient::ConnectionError) }
        end
      end
    end
  end
end
