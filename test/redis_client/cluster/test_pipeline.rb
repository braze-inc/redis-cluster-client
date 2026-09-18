# frozen_string_literal: true

require 'testing_helper'

class RedisClient
  class Cluster
    class TestPipeline < TestingWrapper
      NODE_KEY = '127.0.0.1:6379'
      PRIMARY_NODE_KEY = '127.0.0.1:6380'
      SLOT = 12_345

      # Stands in for the per-node client that +try_asking+ sends ASKING to.
      class FakeAskingClient
        def initialize(router, node_key)
          @router = router
          @node_key = node_key
        end

        def call(command)
          @router.record_asking(@node_key, command)
          'OK'
        end
      end

      class FakeRouter
        attr_accessor :ask_primary_node_key
        attr_reader :redirected_commands, :redirected_targets, :asking_commands, :renew_count

        def initialize
          @redirected_commands = []
          @redirected_targets = []
          @asking_commands = []
          @renew_count = 0
        end

        def deferred_renew_cluster_state!; end

        def renew_cluster_state
          @renew_count += 1
        end

        # Both resolve the target out of the error message, so tests can assert where we went.
        def assign_asking_node(message)
          message.split.last
        end

        def assign_redirection_node(message)
          message.split.last
        end

        def handle_redirection(node, _key, retry_count:) # rubocop:disable Lint/UnusedMethodArgument
          yield(FakeAskingClient.new(self, node))
        end

        def record_asking(node_key, command)
          @asking_commands << [node_key, command]
        end

        def send_command_to_node(node, _method, command, _args)
          @redirected_commands << command
          @redirected_targets << node
          'OK'
        end

        def find_node_key(_command, seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          NODE_KEY
        end

        # Same as find_node_key while building the pipeline so multi + call share a batch.
        # Tests can set ask_primary_node_key before build_stale_cluster_state_redirection.
        def find_primary_node_key(_command)
          ask_primary_node_key || NODE_KEY
        end

        def find_node(_node_key)
          :fake_node
        end

        def find_slot(command)
          name = command.first.to_s.downcase
          return nil if %w[multi exec].include?(name)

          SLOT
        end
      end

      def setup
        @router = FakeRouter.new
        @pipeline = new_pipeline(exception: true)
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

      def test_build_stale_cluster_state_redirection_keeps_command_error_that_follows_cluster_down
        down = cluster_down_error
        other = command_error
        @pipeline.call('SET', 'down-key', '1')
        @pipeline.call('SET', 'bad-key', '2', 'too many args')

        stale = stale_cluster_state([down, other], first_exception: down)
        redirection = build_stale_redirection(stale)

        assert_same(other, redirection.first_exception)
        assert_equal([0], redirection.indices)
      end

      def test_build_stale_cluster_state_redirection_keeps_no_first_exception_without_exception
        pipeline = new_pipeline(exception: false)
        pipeline.call('SET', 'down-key', '1')
        pipeline.call('SET', 'bad-key', '2', 'too many args')

        # exception: false never populates first_exception, so nothing should be raised later.
        stale = stale_cluster_state([cluster_down_error, command_error])
        redirection = build_stale_redirection(stale, pipeline: pipeline)

        assert_nil(redirection.first_exception)
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

      def test_build_stale_cluster_state_redirection_raises_when_node_key_missing
        down = cluster_down_error
        @pipeline.call('SET', 'down-key', '1')
        drop_routing!

        err = assert_raises(::RedisClient::CommandError) do
          build_stale_redirection(stale_cluster_state([down]))
        end

        assert_same(down, err)
      end

      def test_build_stale_cluster_state_redirection_keeps_error_when_node_key_missing_without_exception
        pipeline = new_pipeline(exception: false)
        down = cluster_down_error
        pipeline.call('SET', 'down-key', '1')
        drop_routing!

        redirection = build_stale_redirection(stale_cluster_state([down]), pipeline: pipeline)

        assert_empty(redirection.indices)
        assert_same(down, redirection.replies[0])
      end

      def test_build_stale_cluster_state_redirection_raises_when_multi_has_no_slot
        down = cluster_down_error
        @pipeline.multi do |tx|
          tx.call('SET', 'tx-key', '1')
        end
        @router.define_singleton_method(:find_slot) { |_command| nil }

        err = assert_raises(::RedisClient::CommandError) do
          build_stale_redirection(stale_cluster_state(['OK', 'QUEUED', down]))
        end

        assert_same(down, err)
      end

      def test_execute_raises_command_error_that_follows_cluster_down
        pipeline = new_pipeline(exception: true)
        down = cluster_down_error
        other = command_error

        pipeline.call('SET', 'down-key', '1')
        pipeline.call('SET', 'bad-key', '2', 'too many args')
        stale = stale_cluster_state([down, other], first_exception: down)
        pipeline.define_singleton_method(:do_pipelining) do |_cli, _pl|
          raise stale
        end

        err = assert_raises(::RedisClient::CommandError) { pipeline.execute }

        assert_same(other, err)
        assert_equal([%w[SET down-key 1]], @router.redirected_commands)
      end

      def test_build_stale_cluster_state_redirection_keeps_moved_indices
        moved = moved_error
        @pipeline.call('GET', 'moved-key')
        @pipeline.call('GET', 'down-key')

        needed = redirection_needed([moved, cluster_down_error], indices: [0], first_exception: cluster_down_error)
        redirection = build_stale_redirection(needed)

        assert_equal([0, 1], redirection.indices)
        assert_same(moved, redirection.replies[0])
        assert_ask_error(redirection.replies[1], node_key: NODE_KEY)
        # The MOVED reply is recoverable too, so it must not be kept as a raisable error.
        assert_nil(redirection.first_exception)
      end

      def test_execute_refreshes_and_redirects_cluster_down_alongside_moved
        pipeline = new_pipeline(exception: true)
        pipeline.call('GET', 'moved-key')
        pipeline.call('GET', 'down-key')

        needed = redirection_needed([moved_error, cluster_down_error], indices: [0], first_exception: cluster_down_error)
        raise_from_pipelining!(pipeline, needed)

        got = pipeline.execute

        assert_equal(1, @router.renew_count)
        assert_equal(%w[OK OK], got)
        assert_equal([%w[GET moved-key], %w[GET down-key]], @router.redirected_commands)
        # The MOVED names its own target; the synthesized ASK goes to the refreshed owner.
        assert_equal([PRIMARY_NODE_KEY, NODE_KEY], @router.redirected_targets)
        # Only the synthesized ASK needs ASKING first.
        assert_equal([[NODE_KEY, 'asking']], @router.asking_commands)
      end

      def test_execute_redirects_multi_segment_once_when_moved_accompanies_cluster_down
        pipeline = new_pipeline(exception: true)
        pipeline.multi do |tx|
          tx.call('SET', 'tx-key', '1')
        end
        drop_routing!
        sent = stub_multi_exec_segment!(pipeline, reply: ['OK'])

        # MULTI, SET, EXEC: MOVED on the SET, CLUSTERDOWN on the EXEC.
        needed = redirection_needed(['OK', moved_error, cluster_down_error], indices: [1], first_exception: cluster_down_error)
        raise_from_pipelining!(pipeline, needed)

        got = pipeline.execute

        assert_equal(1, @router.renew_count)
        assert_equal([['OK']], got)
        assert_equal(1, sent.size, 'the segment must be redirected exactly once')
        assert_equal([PRIMARY_NODE_KEY, [['multi'], %w[SET tx-key 1], %w[EXEC]]], sent[0])
      end

      def test_execute_unresolved_cluster_down_returns_error_when_exception_false
        pipeline = new_pipeline(exception: false)
        down = cluster_down_error

        got = execute_with_stale_without_redirect(pipeline, replies: [down])

        assert_equal(1, got.size)
        assert_instance_of(::RedisClient::CommandError, got[0])
        assert_equal(down.message, got[0].message)
      end

      def test_execute_unresolved_cluster_down_raises_when_exception_true
        pipeline = new_pipeline(exception: true)
        down = cluster_down_error

        err = assert_raises(::RedisClient::CommandError) do
          execute_with_stale_without_redirect(
            pipeline,
            replies: [down],
            first_exception: down
          )
        end

        assert_same(down, err)
      end

      private

      def new_pipeline(exception:)
        ::RedisClient::Cluster::Pipeline.new(
          @router,
          ::RedisClient::Cluster::NoopCommandBuilder,
          ::RedisClient::Cluster::ConcurrentWorker.create(model: :none),
          exception: exception
        )
      end

      def node_key(pipeline = @pipeline)
        pipeline.instance_variable_get(:@pipelines).keys.first
      end

      def build_stale_redirection(stale, pipeline: @pipeline)
        pipeline.send(:build_stale_cluster_state_redirection, node_key(pipeline), stale)
      end

      def raise_from_pipelining!(pipeline, error)
        pipeline.define_singleton_method(:do_pipelining) do |_cli, _pl|
          raise error
        end
      end

      # Redirecting a segment needs a real node client, so stub at that boundary instead.
      def stub_multi_exec_segment!(pipeline, reply:)
        sent = []
        pipeline.define_singleton_method(:send_multi_exec_segment) do |node, commands, _blocks|
          sent << [node, commands]
          reply
        end
        sent
      end

      def drop_routing!
        @router.define_singleton_method(:find_node_key) { |_command, seed: nil| nil } # rubocop:disable Lint/UnusedBlockArgument
        @router.define_singleton_method(:find_primary_node_key) { |_command| nil }
      end

      def execute_with_stale_without_redirect(pipeline, replies:, first_exception: nil)
        pipeline.call('SET', 'down-key', '1')
        stale = stale_cluster_state(replies, first_exception: first_exception)

        drop_routing!
        pipeline.define_singleton_method(:do_pipelining) do |_cli, _pl|
          raise stale
        end

        pipeline.execute
      end

      def redirection_needed(replies, indices:, first_exception: nil)
        redirection = ::RedisClient::Cluster::Pipeline::RedirectionNeeded.new
        redirection.replies = replies
        redirection.indices = indices
        redirection.first_exception = first_exception
        redirection.stale_cluster_state = true
        redirection
      end

      def stale_cluster_state(replies, first_exception: nil)
        redirection_needed(replies, indices: [], first_exception: first_exception)
      end

      def cluster_down_error
        ::RedisClient::CommandError.new('CLUSTERDOWN Hash slot not served')
      end

      def command_error
        ::RedisClient::CommandError.new('ERR wrong number of arguments')
      end

      def moved_error
        ::RedisClient::CommandError.new("MOVED #{SLOT} #{PRIMARY_NODE_KEY}")
      end

      def assert_ask_error(result, node_key:)
        assert_instance_of(::RedisClient::CommandError, result)
        assert_equal("ASK #{SLOT} #{node_key}", result.message)
      end
    end
  end
end
