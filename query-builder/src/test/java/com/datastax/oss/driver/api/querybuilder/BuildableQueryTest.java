/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.querybuilder;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.function;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.tuple;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class BuildableQueryTest {

  @DataProvider
  public static Object[][] sampleQueries() {
    // query | values | expected CQL | expected idempotence
    return new Object[][] {
      {
        selectFrom("foo").all().whereColumn("k").isEqualTo(bindMarker()),
        new Object[] {1},
        "SELECT * FROM foo WHERE k=?",
        true
      },
      {
        deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker()),
        new Object[] {1},
        "DELETE FROM foo WHERE k=?",
        true
      },
      {
        deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker()).ifExists(),
        new Object[] {1},
        "DELETE FROM foo WHERE k=? IF EXISTS",
        false
      },
      {
        insertInto("foo").value("a", bindMarker()).value("b", bindMarker()),
        new Object[] {1, "a"},
        "INSERT INTO foo (a,b) VALUES (?,?)",
        true
      },
      {
        insertInto("foo").value("k", tuple(bindMarker(), function("generate_id"))),
        new Object[] {1},
        "INSERT INTO foo (k) VALUES ((?,generate_id()))",
        false
      },
      {
        update("foo").setColumn("v", bindMarker()).whereColumn("k").isEqualTo(bindMarker()),
        new Object[] {3, 1},
        "UPDATE foo SET v=? WHERE k=?",
        true
      },
      {
        update("foo")
            .setColumn("v", function("non_idempotent_func"))
            .whereColumn("k")
            .isEqualTo(bindMarker()),
        new Object[] {1},
        "UPDATE foo SET v=non_idempotent_func() WHERE k=?",
        false
      },
    };
  }

  @Test
  @UseDataProvider("sampleQueries")
  public void should_build_statement(
      BuildableQuery query,
      @SuppressWarnings("unused") Object[] boundValues,
      String expectedQueryString,
      boolean expectedIdempotence) {
    SimpleStatement statement = query.build();
    assertThat(statement.getQuery()).isEqualTo(expectedQueryString);
    assertThat(statement.isIdempotent()).isEqualTo(expectedIdempotence);
    assertThat(statement.getPositionalValues()).isEmpty();
    assertThat(statement.getNamedValues()).isEmpty();
  }

  @Test
  @UseDataProvider("sampleQueries")
  public void should_build_statement_with_values(
      BuildableQuery query,
      Object[] boundValues,
      String expectedQueryString,
      boolean expectedIdempotence) {
    SimpleStatement statement = query.build(boundValues);
    assertThat(statement.getQuery()).isEqualTo(expectedQueryString);
    assertThat(statement.isIdempotent()).isEqualTo(expectedIdempotence);
    assertThat(statement.getPositionalValues()).containsExactly(boundValues);
    assertThat(statement.getNamedValues()).isEmpty();
  }

  @Test
  @UseDataProvider("sampleQueries")
  public void should_convert_to_statement_builder(
      BuildableQuery query,
      Object[] boundValues,
      String expectedQueryString,
      boolean expectedIdempotence) {
    SimpleStatement statement = query.builder().addPositionalValues(boundValues).build();
    assertThat(statement.getQuery()).isEqualTo(expectedQueryString);
    assertThat(statement.isIdempotent()).isEqualTo(expectedIdempotence);
    assertThat(statement.getPositionalValues()).containsExactly(boundValues);
    assertThat(statement.getNamedValues()).isEmpty();
  }
}
