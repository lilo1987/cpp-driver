/*
  Copyright (c) 2014-2016 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "test_token_map_utils.hpp"

#include <boost/test/unit_test.hpp>

namespace {

template <class Partitioner>
struct TestTokenMap {
  typedef typename cass::ReplicationStrategy<Partitioner>::Token Token;
  typedef std::map<Token, cass::Host::Ptr> TokenHostMap;

  TokenHostMap tokens;
  cass::ScopedPtr<cass::TokenMap> token_map;

  TestTokenMap()
    : token_map(cass::TokenMap::from_partitioner(Partitioner::name())) { }

  void populate_token_map() {
    cass::CollectionType::ConstPtr data_type(cass::CollectionType::list(cass::DataType::ConstPtr(new cass::DataType(CASS_VALUE_TYPE_VARINT)), false));
    for (typename TokenHostMap::const_iterator i = tokens.begin(),
         end = tokens.end(); i != end; ++i) {
      TokenCollectionBuilder builder;
      builder.append_token(i->first);
      builder.finish();
      cass::Value value(CASS_PROTOCOL_VERSION, data_type, builder.data(), builder.size());
      token_map->add_host(i->second, &value);
    }
  }

  const cass::Host::Ptr& get_replica(const std::string& key) {
    typename TokenHostMap::const_iterator i = tokens.upper_bound(Partitioner::hash(key));
    if (i != tokens.end()) {
      return i->second;
    } else {
      return tokens.begin()->second;
    }
  }

  void build(const std::string& keyspace_name = "ks", size_t replication_factor = 3) {
    add_keyspace_simple(keyspace_name, replication_factor, token_map.get());
    populate_token_map();
    token_map->build();
  }

  void verify(const std::string& keyspace_name = "ks") {
    const std::string keys[] = { "test", "abc", "def", "a", "b", "c", "d" };

    for (size_t i = 0; i < sizeof(keys)/sizeof(keys[0]); ++i) {
      const std::string& key = keys[i];

      const cass::CopyOnWriteHostVec& hosts = token_map->get_replicas(keyspace_name, key);
      BOOST_REQUIRE(hosts && hosts->size() > 0);

      const cass::Host::Ptr& host = get_replica(key);
      BOOST_REQUIRE(host);

      BOOST_CHECK_EQUAL(hosts->front()->address(), host->address());
    }
  }
};

} // namespace

BOOST_AUTO_TEST_SUITE(token_map)

BOOST_AUTO_TEST_CASE(murmur3)
{
  TestTokenMap<cass::Murmur3Partitioner> test_token_map;

  test_token_map.tokens[CASS_INT64_MIN / 2] = create_host("1.0.0.1");
  test_token_map.tokens[0]                  = create_host("1.0.0.2");
  test_token_map.tokens[CASS_INT64_MAX / 2] = create_host("1.0.0.3");

  test_token_map.build();
  test_token_map.verify();
}

BOOST_AUTO_TEST_CASE(murmur3_multiple_tokens_per_host)
{
  TestTokenMap<cass::Murmur3Partitioner> test_token_map;

  const size_t tokens_per_host = 256;

  cass::HostVec hosts;
  hosts.push_back(create_host("1.0.0.1"));
  hosts.push_back(create_host("1.0.0.2"));
  hosts.push_back(create_host("1.0.0.3"));
  hosts.push_back(create_host("1.0.0.4"));

  MT19937_64 rng;

  for (cass::HostVec::iterator i = hosts.begin(); i != hosts.end(); ++i) {
    for (size_t j = 0; j < tokens_per_host; ++j) {
      test_token_map.tokens[rng()] = *i;
    }
  }

  test_token_map.build();
  test_token_map.verify();
}

BOOST_AUTO_TEST_CASE(random)
{
  cass::ScopedPtr<cass::TokenMap> token_map(cass::TokenMap::from_partitioner(cass::RandomPartitioner::name()));

  TestTokenMap<cass::RandomPartitioner> test_token_map;

  test_token_map.tokens[create_random_token("42535295865117307932921825928971026432")] = create_host("1.0.0.1");  // 2^127 / 4
  test_token_map.tokens[create_random_token("85070591730234615865843651857942052864")] = create_host("1.0.0.2");  // 2^127 / 2
  test_token_map.tokens[create_random_token("1605887595351923798765477786913079296")]  = create_host("1.0.0.3"); // 2^127 * 3 / 4

  test_token_map.build();
  test_token_map.verify();
}

BOOST_AUTO_TEST_CASE(byte_ordered)
{
  cass::ScopedPtr<cass::TokenMap> token_map(cass::TokenMap::from_partitioner(cass::ByteOrderedPartitioner::name()));

  TestTokenMap<cass::ByteOrderedPartitioner> test_token_map;

  test_token_map.tokens[create_byte_ordered_token("g")] = create_host("1.0.0.1");
  test_token_map.tokens[create_byte_ordered_token("m")] = create_host("1.0.0.2");
  test_token_map.tokens[create_byte_ordered_token("s")] = create_host("1.0.0.3");

  test_token_map.build();
  test_token_map.verify();
}

BOOST_AUTO_TEST_CASE(remove_host)
{
  TestTokenMap<cass::Murmur3Partitioner> test_token_map;

  test_token_map.tokens[CASS_INT64_MIN / 2] = create_host("1.0.0.1");
  test_token_map.tokens[0]                  = create_host("1.0.0.2");
  test_token_map.tokens[CASS_INT64_MAX / 2] = create_host("1.0.0.3");

  test_token_map.build("ks", 2);
  test_token_map.verify();

  cass::TokenMap* token_map = test_token_map.token_map.get();

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 2);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.1", 9042));
    BOOST_CHECK_EQUAL((*replicas)[1]->address(), cass::Address("1.0.0.2", 9042));
  }

  TestTokenMap<cass::Murmur3Partitioner>::TokenHostMap::iterator host_to_remove_it = test_token_map.tokens.begin();

  token_map->remove_host(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 2);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.2", 9042));
    BOOST_CHECK_EQUAL((*replicas)[1]->address(), cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map->remove_host(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("ks", "abc");

    BOOST_REQUIRE(replicas && replicas->size() == 1);
    BOOST_CHECK_EQUAL((*replicas)[0]->address(), cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map->remove_host(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map->get_replicas("test", "abc");

    BOOST_CHECK(!replicas);
  }

}

BOOST_AUTO_TEST_SUITE_END()

#if 0

BOOST_AUTO_TEST_CASE(remove_host)
{
  TestTokenMap<int64_t> test_remove_host;

  test_remove_host.strategy =
      cass::SharedRefPtr<cass::ReplicationStrategy>(new cass::SimpleStrategy("", 2));

  test_remove_host.tokens[CASS_INT64_MIN / 2] = create_host("1.0.0.1");
  test_remove_host.tokens[0] = create_host("1.0.0.2");
  test_remove_host.tokens[CASS_INT64_MAX / 2] = create_host("1.0.0.3");

  test_remove_host.build(cass::Murmur3Partitioner::PARTITIONER_CLASS, "test");

  cass::TokenMap& token_map = test_remove_host.token_map;

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 2);
    BOOST_CHECK((*replicas)[0]->address() == cass::Address("1.0.0.1", 9042));
    BOOST_CHECK((*replicas)[1]->address() == cass::Address("1.0.0.2", 9042));
  }

  TestTokenMap<int64_t>::TokenHostMap::iterator host_to_remove_it = test_remove_host.tokens.begin();

  token_map.remove_host(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 2);
    BOOST_CHECK((*replicas)[0]->address() == cass::Address("1.0.0.2", 9042));
    BOOST_CHECK((*replicas)[1]->address() == cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map.remove_host(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 1);
    BOOST_CHECK((*replicas)[0]->address() == cass::Address("1.0.0.3", 9042));
  }

  ++host_to_remove_it;
  token_map.remove_host(host_to_remove_it->second);

  {
    const cass::CopyOnWriteHostVec& replicas = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 0);
  }
}

BOOST_AUTO_TEST_CASE(drop_keyspace)
{
  TestTokenMap<int64_t> test_drop_keyspace;

  test_drop_keyspace.strategy =
      cass::SharedRefPtr<cass::ReplicationStrategy>(new cass::SimpleStrategy("", 2));

  test_drop_keyspace.tokens[CASS_INT64_MIN / 2] = create_host("1.0.0.1");
  test_drop_keyspace.tokens[0] = create_host("1.0.0.2");
  test_drop_keyspace.tokens[CASS_INT64_MAX / 2] = create_host("1.0.0.3");

  test_drop_keyspace.build(cass::Murmur3Partitioner::PARTITIONER_CLASS, "test");

  cass::TokenMap& token_map = test_drop_keyspace.token_map;

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 2);
    BOOST_CHECK((*replicas)[0]->address() == cass::Address("1.0.0.1", 9042));
    BOOST_CHECK((*replicas)[1]->address() == cass::Address("1.0.0.2", 9042));
  }

  token_map.drop_keyspace("test");

  {
    const cass::CopyOnWriteHostVec& replicas
        = token_map.get_replicas("test", "abc");

    BOOST_REQUIRE(replicas->size() == 0);
  }
}



BOOST_AUTO_TEST_SUITE_END()

#endif
