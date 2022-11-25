/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package training;

import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;


class BatchPlainStatementsExecutor<T> implements JdbcBatchStatementExecutor<T>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(BatchPlainStatementsExecutor.class);

    private final List<T> batch;
    private final PlainSqlFactory<T> sqlFactory;

    private transient Statement st;
    private Set<Long> threads = new HashSet<>();

    BatchPlainStatementsExecutor(PlainSqlFactory<T> sqlFactory) {
        this.batch = new ArrayList<>();
        this.sqlFactory = sqlFactory;
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.st = connection.createStatement();
    }

    @Override
    public void addToBatch(T record) {
        threads.add(Thread.currentThread().getId());
        batch.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {

        if (!batch.isEmpty()) {
            for (T r : batch) {
                st.addBatch(sqlFactory.apply(r));
            }
            st.executeBatch();
            batch.clear();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (st != null) {
            st.close();
            st = null;
        }

    }

    public interface PlainSqlFactory<T> extends Function<T, String>, Serializable {
    }
}
