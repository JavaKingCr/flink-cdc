/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.utils;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Utilities to discovery matched collections. */
public class CollectionDiscoveryUtils {

    public static final String REGEX_META_CHARACTERS = ".$|()[]{}<>^?*+-=!\\";

    public static final String ADD_NS_FIELD_NAME = "_ns_";

    public static final Bson ADD_NS_FIELD =
            BsonDocument.parse(
                    String.format(
                            "{'$addFields': {'%s': {'$concat': ['$ns.db', '.', '$ns.coll']}}}",
                            ADD_NS_FIELD_NAME));

    private CollectionDiscoveryUtils() {}

    public static List<String> databaseNames(
            MongoClient mongoClient, Predicate<String> databaseFilter) {
        List<String> databaseNames = new ArrayList<>();
        mongoClient
                .listDatabaseNames()
                .forEach(
                        dbName -> {
                            if (databaseFilter.test(dbName)) {
                                databaseNames.add(dbName);
                            }
                        });
        return databaseNames;
    }

    public static List<String> collectionNames(
            MongoClient mongoClient,
            List<String> databaseNames,
            Predicate<String> collectionFilter) {
        List<String> collectionNames = new ArrayList<>();
        for (String dbName : databaseNames) {
            MongoDatabase db = mongoClient.getDatabase(dbName);
            db.listCollectionNames()
                    .map(collName -> dbName + "." + collName)
                    .forEach(
                            fullName -> {
                                if (collectionFilter.test(fullName)) {
                                    collectionNames.add(fullName);
                                }
                            });
        }
        return collectionNames;
    }

    public static Predicate<String> databaseFilter(List<String> databaseList) {
        Predicate<String> databaseFilter = CollectionDiscoveryUtils::isNotBuiltInDatabase;
        if (databaseList != null && !databaseList.isEmpty()) {
            List<Pattern> databasePatterns = includeListAsPatterns(databaseList);
            databaseFilter = databaseFilter.and(anyMatch(databasePatterns));
        }
        return databaseFilter;
    }

    public static Predicate<String> collectionsFilter(List<String> collectionList) {
        Predicate<String> collectionFilter = CollectionDiscoveryUtils::isNotBuiltInCollections;
        if (collectionList != null && !collectionList.isEmpty()) {
            List<Pattern> collectionPatterns = includeListAsPatterns(collectionList);
            collectionFilter = collectionFilter.and(anyMatch(collectionPatterns));
        }
        return collectionFilter;
    }

    public static Predicate<String> anyMatch(List<Pattern> patterns) {
        return s -> {
            for (Pattern p : patterns) {
                if (p.matcher(s).matches()) {
                    return true;
                }
            }
            return false;
        };
    }

    public static List<Pattern> includeListAsPatterns(List<String> includeList) {
        if (includeList != null && !includeList.isEmpty()) {
            return includeList.stream()
                    // Notice that MongoDB's database and collection names are case-sensitive.
                    // Please refer to https://docs.mongodb.com/manual/reference/limits/
                    // We use case-sensitive pattern here to avoid unexpected results.
                    .map(CollectionDiscoveryUtils::completionPattern)
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    public static boolean isIncludeListExplicitlySpecified(
            List<String> includeList, List<String> discoveredList) {
        if (includeList == null || includeList.size() != 1) {
            return false;
        }
        if (discoveredList == null || discoveredList.size() != 1) {
            return false;
        }
        String firstOfIncludeList = includeList.get(0);
        String firstOfDiscoveredList = discoveredList.get(0);
        return firstOfDiscoveredList.equals(firstOfIncludeList);
    }

    public static boolean isNotBuiltInCollections(String fullName) {
        if (fullName == null) {
            return false;
        }
        MongoNamespace namespace = new MongoNamespace(fullName);
        return isNotBuiltInDatabase(namespace.getDatabaseName())
                && !namespace.getCollectionName().startsWith("system.");
    }

    public static boolean isNotBuiltInDatabase(String databaseName) {
        if (databaseName == null) {
            return false;
        }
        return !"local".equals(databaseName)
                && !"admin".equals(databaseName)
                && !"config".equals(databaseName);
    }

    public static boolean containsRegexMetaCharacters(String literal) {
        if (StringUtils.isEmpty(literal)) {
            return false;
        }
        for (int i = 0; i < literal.length(); i++) {
            if (REGEX_META_CHARACTERS.indexOf(literal.charAt(i)) != -1) {
                return true;
            }
        }
        return false;
    }

    public static Pattern completionPattern(String pattern) {
        if (pattern.startsWith("^") && pattern.endsWith("$")) {
            return Pattern.compile(pattern);
        }
        return Pattern.compile("^(" + pattern + ")$");
    }

    public static String bsonListToJson(List<Bson> bsonList) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (Bson bson : bsonList) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(bson.toBsonDocument().toJson());
        }
        sb.append("]");
        return sb.toString();
    }
}
