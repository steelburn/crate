/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze.relations;

import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public abstract class AbstractTableRelation<T extends TableInfo> implements AnalyzedRelation, FieldResolver {

    protected final T tableInfo;
    private final List<Symbol> outputs;
    private final List<Symbol> hiddenOutputs;

    public AbstractTableRelation(T tableInfo, List<Symbol> outputs, List<Symbol> hiddenOutputs) {
        this.tableInfo = tableInfo;
        this.outputs = outputs;
        this.hiddenOutputs = hiddenOutputs;
    }

    public T tableInfo() {
        return tableInfo;
    }

    @Nonnull
    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public List<Symbol> hiddenOutputs() {
        return hiddenOutputs;
    }

    @Nullable
    public SimpleReference getField(ColumnIdent path) {
        return tableInfo.getReadReference(path);
    }

    @Override
    public RelationName relationName() {
        return tableInfo.ident();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '{' + this.tableInfo.ident() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractTableRelation<?> that = (AbstractTableRelation<?>) o;

        return tableInfo.equals(that.tableInfo);
    }

    @Override
    public int hashCode() {
        return tableInfo.hashCode();
    }

    @Override
    @Nullable
    public SimpleReference resolveField(ScopedSymbol field) {
        if (field.relation().equals(tableInfo.ident())) {
            return getField(field.column());
        }
        return null;
    }
}
