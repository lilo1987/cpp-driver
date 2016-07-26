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

#include <limits>
#include <sstream>
#include <string>
#include <stdarg.h>

BOOST_AUTO_TEST_SUITE(token_metadata)

BOOST_AUTO_TEST_CASE(murmur3)
{
  cass::ScopedPtr<cass::TokenMap> token_map(cass::TokenMap::from_partitioner(cass::Murmur3Partitioner::name()));
  MT19937_64 rng;

  char host[128];
  char dc[128];

  ReplicationMap replication;
  for (int i = 1; i <= 10; ++i) {
    sprintf(host, "127.0.%d.%d", i / 255, i % 255);
    sprintf(dc, "dc%d", (i % 20) + 1);
    replication[dc] = "3";
    add_murmur3_host(create_host(host, "rack1", dc), rng, 256, token_map.get());
  }
  //add_murmur3_host(create_host("127.0.0.2", "rack1", "dc1"), rng, 2048, token_map.get());
  //add_murmur3_host(create_host("127.0.0.3", "rack1", "dc1"), rng, 2048, token_map.get());
  //add_murmur3_host(create_host("127.0.0.4", "rack1", "dc1"), rng, 2048, token_map.get());
  //add_murmur3_host(create_host("127.0.0.5", "rack1", "dc1"), rng, 2048, token_map.get());
  //add_murmur3_host(create_host("127.0.0.6", "rack1", "dc1"), rng, 2048, token_map.get());

  add_keyspace_network_topology("ks1", replication, token_map.get());

  uint64_t start = uv_hrtime();
  token_map->build();
  uint64_t elapsed = uv_hrtime() - start;

  // HERE
  //token_map->get_replicas("ks1", )

  printf("Elapsed: %f ms\n", (double)elapsed / (1000.0 * 1000.0));
}

BOOST_AUTO_TEST_SUITE_END()
