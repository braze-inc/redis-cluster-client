# frozen_string_literal: true

require 'testing_helper'
require 'redis_client/cluster/node/testing_topology_mixin'

class RedisClient
  class Cluster
    class Node
      class TestPrimaryOnly < TestingWrapper
        TESTING_TOPOLOGY_OPTIONS = { replica: false }.freeze
        include TestingTopologyMixin

        def test_clients_with_redis_client
          got = @test_node.clients
          got.each do |client|
            assert_instance_of(::RedisClient, client)
            assert_equal('master', client.call('ROLE').first)
          end
        end

        def test_clients_with_pooled_redis_client
          test_node = make_node(pool: { timeout: 3, size: 2 })
          got = test_node.clients
          got.each do |client|
            assert_instance_of(::RedisClient::Pooled, client)
            assert_equal('master', client.call('ROLE').first)
          end
        end

        def test_primary_clients
          got = @test_node.primary_clients
          got.each do |client|
            assert_instance_of(::RedisClient, client)
            assert_equal('master', client.call('ROLE').first)
          end
        end

        def test_replica_clients
          got = @test_node.replica_clients
          got.each do |client|
            assert_instance_of(::RedisClient, client)
            assert_equal('master', client.call('ROLE').first)
          end
        end

        def test_clients_for_scanning
          got = @test_node.clients_for_scanning
          got.each do |client|
            assert_instance_of(::RedisClient, client)
            assert_equal('master', client.call('ROLE').first)
          end
        end

        def test_find_node_key_of_replica
          want = 'dummy_key'
          got = @test_topology.find_node_key_of_replica('dummy_key')
          assert_equal(want, got)
        end

        def test_any_primary_node_key
          got = @test_topology.any_primary_node_key
          assert_includes(@replications.keys, got)
        end

        def test_any_replica_node_key
          got = @test_topology.any_replica_node_key
          assert_includes(@replications.keys, got)
        end

        def test_lazy_connect_preserves_routing_invariants
          replica_info = @test_node.instance_variable_get(:@node_info).find(&:replica?)
          replica_key = replica_info.node_key
          primary_keys = @replications.keys.sort

          refute(@test_topology.clients.key?(replica_key))
          @test_node.find_by(replica_key)

          assert(@test_topology.clients.key?(replica_key), 'lazy-connected replica should be in clients')
          refute(@test_topology.instance_variable_get(:@primary_clients).key?(replica_key),
                 'lazy-connected replica should not be in partitioned primary_clients')

          assert_equal(primary_keys, @test_topology.clients_for_scanning.keys.sort)

          @test_node.clients_for_scanning.each do |client|
            assert_equal('master', client.call('ROLE').first)
          end

          @test_node.replica_clients.each do |client|
            assert_equal('master', client.call('ROLE').first)
          end

          sample_primary = primary_keys.first
          assert_equal(sample_primary, @test_topology.find_node_key_of_replica(sample_primary))
        end
      end
    end
  end
end
