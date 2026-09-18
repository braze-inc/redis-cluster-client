# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class Cluster
    class TestPipeline < TestingWrapper
      NODE_KEY = '127.0.0.1:6379'
      PRIMARY_NODE_KEY = '127.0.0.1:6380'
      SLOT = 12_345

      class FakeRouter
        attr_accessor :ask_primary_node_key

        def deferred_renew_cluster_state!; end

        def find_node_key(_command, seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          NODE_KEY
        end

        # Same as find_node_key while building the pipeline so multi + call share a batch.
        # Tests can set ask_primary_node_key before build_stale_cluster_state_redirection.
        def find_primary_node_key(_command)
          ask_primary_node_key || NODE_KEY
        end

        def find_node(_node_key)
          nil
        end

        def find_slot(command)
          name = command.first.to_s.downcase
          return nil if %w[multi exec].include?(name)

          SLOT
        end
      end

      def setup
        @router = FakeRouter.new
        @pipeline = ::RedisClient::Cluster::Pipeline.new(
          @router,
          ::RedisClient::Cluster::NoopCommandBuilder,
          ::RedisClient::Cluster::ConcurrentWorker.create(model: :none),
          exception: true
        )
      end

      def test_build_stale_cluster_state_redirection_asks_only_cluster_down_commands
        @pipeline.call('SET', 'ok-key', '1')
        @pipeline.call('SET', 'down-key', '2')

        stale = stale_cluster_state(['OK', cluster_down_error])
        redirection = build_stale_redirection(stale)

        assert_instance_of(::RedisClient::Cluster::Pipeline::RedirectionNeeded, redirection)
        assert_equal([1], redirection.indices)
        assert_equal('OK', redirection.replies[0])
        assert_ask_error(redirection.replies[1], node_key: NODE_KEY)
        assert_nil(redirection.first_exception)
      end

      def test_build_stale_cluster_state_redirection_clears_cluster_down_first_exception
        @pipeline.call('SET', 'down-key', '1')

        stale = stale_cluster_state(
          [cluster_down_error],
          first_exception: cluster_down_error
        )
        redirection = build_stale_redirection(stale)

        assert_nil(redirection.first_exception)
        assert_equal([0], redirection.indices)
      end

      def test_build_stale_cluster_state_redirection_keeps_non_cluster_down_first_exception
        @pipeline.call('SET', 'down-key', '1')
        other = ::RedisClient::CommandError.new('ERR boom')

        stale = stale_cluster_state([cluster_down_error], first_exception: other)
        redirection = build_stale_redirection(stale)

        assert_same(other, redirection.first_exception)
      end

      def test_build_stale_cluster_state_redirection_retries_multi_when_exec_is_cluster_down
        @pipeline.multi do |tx|
          tx.call('SET', 'tx-key', '1')
        end
        @router.ask_primary_node_key = PRIMARY_NODE_KEY

        # MULTI, SET, EXEC
        stale = stale_cluster_state(['OK', 'QUEUED', cluster_down_error])
        redirection = build_stale_redirection(stale)

        assert_equal([1], redirection.indices)
        assert_equal('OK', redirection.replies[0])
        assert_ask_error(redirection.replies[1], node_key: PRIMARY_NODE_KEY)
        assert_equal(cluster_down_error.message, redirection.replies[2].message)
      end

      def test_build_stale_cluster_state_redirection_skips_multi_without_cluster_down
        @pipeline.multi do |tx|
          tx.call('SET', 'tx-key', '1')
        end
        @pipeline.call('SET', 'down-key', '2')

        stale = stale_cluster_state(['OK', 'QUEUED', ['OK'], cluster_down_error])
        redirection = build_stale_redirection(stale)

        assert_equal([3], redirection.indices)
        assert_equal('OK', redirection.replies[0])
        assert_equal('QUEUED', redirection.replies[1])
        assert_equal(['OK'], redirection.replies[2])
        assert_ask_error(redirection.replies[3], node_key: NODE_KEY)
      end

      def test_build_stale_cluster_state_redirection_skips_when_node_key_missing
        @pipeline.call('SET', 'down-key', '1')
        @router.define_singleton_method(:find_node_key) { |_command, seed: nil| nil } # rubocop:disable Lint/UnusedMethodArgument

        stale = stale_cluster_state([cluster_down_error])
        redirection = build_stale_redirection(stale)

        assert_empty(redirection.indices)
        assert_equal(cluster_down_error.message, redirection.replies[0].message)
      end

      private

      def node_key
        @pipeline.instance_variable_get(:@pipelines).keys.first
      end

      def build_stale_redirection(stale)
        @pipeline.send(:build_stale_cluster_state_redirection, node_key, stale)
      end

      def stale_cluster_state(replies, first_exception: nil)
        stale = ::RedisClient::Cluster::Pipeline::StaleClusterState.new
        stale.replies = replies
        stale.first_exception = first_exception
        stale
      end

      def cluster_down_error
        ::RedisClient::CommandError.new('CLUSTERDOWN Hash slot not served')
      end

      def assert_ask_error(result, node_key:)
        assert_instance_of(::RedisClient::CommandError, result)
        assert_equal("ASK #{SLOT} #{node_key}", result.message)
      end
    end
  end
end
