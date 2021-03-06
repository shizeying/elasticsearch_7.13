/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TypeFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.singletonMap;

@SuppressWarnings({"unchecked", "rawtypes"})
public class LeafStoredFieldsLookup implements Map<Object, FieldLookup> {

    private final Function<String, MappedFieldType> fieldTypeLookup;
    private final CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> reader;

    private int docId = -1;

    private final Map<String, FieldLookup> cachedFieldData = new HashMap<>();

    LeafStoredFieldsLookup(Function<String, MappedFieldType> fieldTypeLookup,
                           CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> reader) {
        this.fieldTypeLookup = fieldTypeLookup;
        this.reader = reader;
    }

    public void setDocument(int docId) {
        if (this.docId == docId) { // if we are called with the same docId, don't invalidate source
            return;
        }
        this.docId = docId;
        clearCache();
    }

    @Override
    public FieldLookup get(Object key) {
        return loadFieldData(key.toString());
    }

    @Override
    public boolean containsKey(Object key) {
        try {
            loadFieldData(key.toString());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldLookup put(Object key, FieldLookup value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldLookup remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    private FieldLookup loadFieldData(String name) {
        FieldLookup data = cachedFieldData.get(name);
        if (data == null) {
            MappedFieldType fieldType = fieldTypeLookup.apply(name);
            if (fieldType == null) {
                throw new IllegalArgumentException("No field found for [" + name + "] in mapping");
            }
            data = new FieldLookup(fieldType);
            cachedFieldData.put(name, data);
        }
        if (data.fields() == null) {
            MappedFieldType fieldType = data.fieldType();
            List<Object> values;
            if (TypeFieldMapper.NAME.equals(fieldType.name())) {
                TypeFieldMapper.emitTypesDeprecationWarning();
                values = Collections.singletonList(((TypeFieldMapper.TypeFieldType)fieldType).getType());
            } else {
                values = new ArrayList<>(2);
                SingleFieldsVisitor visitor = new SingleFieldsVisitor(fieldType, values);
                try {
                    reader.accept(docId, visitor);
                } catch (IOException e) {
                    throw new ElasticsearchParseException("failed to load field [{}]", e, name);
                }
            }
            data.fields(singletonMap(fieldType.name(), values));
        }
        return data;
    }

    private void clearCache() {
        if (cachedFieldData.isEmpty()) {
            /*
             * This code is in the hot path for things like ScoreScript and
             * runtime fields but the map is almost always empty. So we
             * bail early then instead of building the entrySet.
             */
            return;
        }
        for (Entry<String, FieldLookup> entry : cachedFieldData.entrySet()) {
            entry.getValue().clear();
        }
    }

}
