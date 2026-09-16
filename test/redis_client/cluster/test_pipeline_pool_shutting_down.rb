# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class Cluster
    class TestPipelinePoolShuttingDown < TestingWrapper
      def setup
        @captured_commands = ::Middlewares::CommandCapture::CommandBuffer.new
        @redirect_count = ::Middlewares::RedirectCount::Counter.new
        @reload_attempts = []
      end

      def teardown
        @client&.call('FLUSHDB')
        wait_for_replication(@client)
        @client&.close
      end

      def test_pooled_pipeline_recovers_from_pool_shutting_down
        @trigger = ::Middlewares::PoolShuttingDownOnce::Trigger.new
        @client = build_client(
          middleware: ::Middlewares::PoolShuttingDownOnce,
          trigger_key: :pool_shutting_down_once,
          trigger: @trigger
        )
        prepare_client!

        key = 'pool_shutdown:test'
        @client.call('SET', key, 'expected')
        wait_for_replication(@client)
        @captured_commands.clear

        got = @client.pipelined { |pi| pi.call('GET', key) }

        assert_equal(['expected'], got)
        assert_equal(1, @reload_attempts.size, 'recovery should retry exactly once via try_reload!')
        assert(@trigger.fired?, 'middleware should have fired exactly once')
        assert_redirect_count_within_limit
      end

      def test_pooled_keys_renews_cluster_state_on_cluster_down
        @trigger = ::Middlewares::ClusterDownOnce::Trigger.new
        @client = build_client(
          middleware: ::Middlewares::ClusterDownOnce,
          trigger_key: :cluster_down_once,
          trigger: @trigger
        )
        prepare_client!

        key = 'cluster_down:test'
        @client.call('SET', key, 'expected')
        wait_for_replication(@client)
        @trigger.reset!
        @reload_attempts.clear
        @captured_commands.clear

        # KEYS is a dedicated command routed to all replicas; failures surface as
        # ErrorCollection on send_command, not via handle_redirection.
        err = assert_raises(::RedisClient::Cluster::ErrorCollection) do
          @client.call('KEYS', 'cluster_down:*')
        end
        assert(
          err.errors.values.any? { |e| e.message.start_with?('CLUSTERDOWN') },
          "expected CLUSTERDOWN in ErrorCollection, got: #{err.errors.values.inspect}"
        )
        assert_equal(1, @reload_attempts.size, 'send_command should renew cluster state via try_reload!')
        assert(@trigger.fired?, 'middleware should have fired exactly once')

        got = @client.call('KEYS', 'cluster_down:*')
        assert_equal([key], got)
      end

      private

      def prepare_client!
        @client.call('FLUSHDB')
        wait_for_replication(@client)
        @trigger.reset! if @trigger.respond_to?(:reset!)
        @captured_commands.clear
        @redirect_count.clear
        @reload_attempts.clear
      end

      def build_client(middleware:, trigger_key:, trigger:)
        config = ::RedisClient::ClusterConfig.new(
          nodes: TEST_NODE_URIS,
          fixed_hostname: TEST_FIXED_HOSTNAME,
          middlewares: [
            ::Middlewares::CommandCapture,
            middleware,
            ::Middlewares::RedirectCount
          ],
          custom: {
            captured_commands: @captured_commands,
            trigger_key => trigger,
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
          reload_attempts << kwargs
          original_try_reload.call(**kwargs)
        end
      end

      def assert_redirect_count_within_limit
        redirects = @redirect_count.get
        assert_operator(
          redirects.moved + redirects.ask,
          :<=,
          1,
          "at most one redirection retry, got: #{redirects.inspect}"
        )
      end

      def wait_for_replication(client)
        client_side_timeout = TEST_TIMEOUT_SEC + 1.0
        server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
        swap_timeout(client, timeout: 0.1) do |cli|
          cli&.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
        rescue ::RedisClient::Cluster::ErrorCollection => e
          raise unless e.errors.values.all? { |err| err.is_a?(::RedisClient::ConnectionError) }
        end
      end
    end
  end
end
