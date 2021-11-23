
//  COPYRIGHT:       PrimeObjects Software Inc. (C) 2021 All Right Reserved
//  COMPANY URL:     https://www.primeobjects.com/
//  CONTACT:         developer@primeobjects.com
// 
//  This source is subject to the DouHub License Agreements. 
// 
//  Our EULAs define the terms of use and license for each DouHub product. 
//  Whenever you install a DouHub product or research DouHub source code file, you will be prompted to review and accept the terms of our EULA. 
//  If you decline the terms of the EULA, the installation should be aborted and you should remove any and all copies of our products and source code from your computer. 
//  If you accept the terms of our EULA, you must abide by all its terms as long as our technologies are being employed within your organization and within your applications.
// 
//  THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY
//  OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT
//  LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
//  FITNESS FOR A PARTICULAR PURPOSE.
// 
//  ALL OTHER RIGHTS RESERVED

import _ from './base';
import { processQuery } from './elastic-search-query-processor.js';

const goodIndexes = {};

export const search = async (context, query, skipCheckSecurity) => {


    let result = [];

    if (_.trackLibs) console.log({ query: JSON.stringify(query) });

    query = processQuery(context, query, skipCheckSecurity);

    if (_.trackLibs) console.log({ query: JSON.stringify(query) });

    result = await _.elasticQuery.query(query);

    result = {
        data: _.map(result.body.hits.hits, (r) => {
            const data = r['_source'];
            data.highlight = r.highlight;
            return data;
        })
    };

    return result;

};

//upsert will have no permission check, it is simply a base function to be called with fully trust
export const upsertRecord = async (rawData) => {

    if (!_.isObject(rawData)) throw 'The data is not provided.';
    const data = _.cloneDeep(rawData);
    const entityName = data.entityName;
    const entityType = data.entityType;
    const id = data.id;

    if (!_.isNonEmptyString(entityName)) throw 'The entityName is not provided.';
    if (!_.isNonEmptyString(id)) throw 'The id is not provided.';

    if (_.trackLibs) console.log({ name: 'elastic-search-upsert', data: JSON.stringify(data) });

    //need to clean up some fields that will messup elastic search
    delete data['_rid'];
    delete data['_attachments'];
    delete data['_self'];
    delete data['_etag'];
    delete data['_ts'];

    //The fields that has been merged into the searchDisplay and searchContent does not need to be kept
    _.each([
        { name: 'description' },
        { name: 'note' },
        { name: 'summary' },
        { name: 'introduction' },
        { name: 'title' },
        { name: 'firstName' },
        { name: 'lastName' },
        { name: 'content' },
        { name: 'name' },
        { name: 'token' },
        { name: 'url' }
    ], (f) => {
        delete data[f.name];
    });

    await checkAndCreateIndex(data.entityName, data.entityType);

    if (_.trackLibs) console.log({ name: 'elastic-search-upsert', data: JSON.stringify(data) });

    //we will always have an index at entityName level
    await _.elasticUpsert(entityName.toLowerCase(), data);

    //if there entityType, we will also index the record in entityType index
    if (_.isNonEmptyString(entityType)) {
        await _.elasticUpsert(`${entityName}_${entityType}`.toLowerCase(), data);
    }

    return data;

};

//upsert will have no permission check, it is simply a base function to be called with fully trust
export const deleteRecord = async (data) => {

    if (!_.isObject(data)) throw 'The data is not provided.';

    const entityName = data.entityName;
    const entityType = data.entityType;
    const id = data.id;

    if (!_.isNonEmptyString(entityName)) throw 'The entityName is not provided.';
    if (!_.isNonEmptyString(id)) throw 'The id is not provided.';

    if (_.trackLibs) console.log({ data: JSON.stringify(data) });

    //we will always have an index at entityName level
    await _.elasticDelete(entityName.toLowerCase(), id);

    //if there entityType, we will also index the record in entityType index
    await _.elasticDelete(`${entityName}_${entityType}`.toLowerCase(), id);

    return data;

};

export const checkAndCreateIndex = async (entityName, entityType, forceCreate) => {

    if (!_.isNonEmptyString(entityName)) throw 'The entityName is not provided.';

    const entityNameIndexName = entityName.toLowerCase();
    const entityTypeIndexName = `${entityName}_${entityType}`.toLowerCase();

    if (forceCreate || !await hasGoodIndex(entityNameIndexName)) {
        await createIndex(entityNameIndexName);
    }

    if (_.isNonEmptyString(entityType) && (forceCreate || !await hasGoodIndex(entityTypeIndexName))) {
        await createIndex(entityTypeIndexName);
    }
};

export const hasGoodIndex = async (indexName) => {

    if (goodIndexes[indexName]) return true;

    if (_.trackLibs) console.log(`hasGoogIndex - ${indexName}`);

    const searchClient = await _.elasticSearch();
    let result = true;
    try {
        const mappings = (await searchClient.indices.getMapping({ index: indexName })).body[indexName].mappings;
        if (_.trackLibs) console.log({ mappings: JSON.stringify(mappings) });
        if (mappings.properties.id.type !== 'keyword') {
            result = false;
        }
    }
    catch (error) {
        console.error({ error: _.isObject(error) ? JSON.stringify(error) : error });
        result = true;
    }

    if (!result) {
        if (_.trackLibs) console.log({ name: 'elastic-search-has-no-index', indexName });
    }
    else {
        goodIndexes[indexName] = true;
    }

    return result;
};

export const createIndex = async (indexName) => {
    //We will need to make some fields not analyzed so we can use exact match when query
    //To prevent this from happening, we need to tell elastic that it is an exact value and it shouldn’t be analyzed to split into tokens.

    const searchClient = await _.elasticSearch();
    if (_.trackLibs) console.log({ name: 'elastic-search-checking-exist-index', indexName });
    let indexExists = await searchClient.indices.exists({ index: indexName });
    if (_.trackLibs) console.log({ name: 'elastic-search-checked-exist-index', indexName, indexExists });
    if (_.isObject(indexExists)) indexExists = indexExists.body;

    //delete first
    if (indexExists) {
        //force recreate, we will need delete first
        if (_.trackLibs) console.log({ name: 'elastic-search-deleting-index', indexName });
        await searchClient.indices.delete({ index: indexName });
        if (_.trackLibs) console.log({ name: 'elastic-search-deleted-index', indexName });
    }

    //The content of the following fields have been merged into searchDisplay or searchContent fields
    //There is also fields such such as token, url should nbot be indexed
    //Therefore these fields not need to be indexed
    // const nonIndexFields = [
    //     { name: 'description' },
    //     { name: 'note' },
    //     { name: 'summary' },
    //     { name: 'introduction' },
    //     { name: 'abstract' },
    //     { name: 'title' },
    //     { name: 'firstName' },
    //     { name: 'lastName' },
    //     { name: 'name' },
    //     { name: 'token' },
    //     { name: 'url' }
    // ];

    //create
    const createParam = {
        index: indexName,
        body: {
            settings: {
                "analysis": {
                    "analyzer": {
                        "platform_analyzer_text": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "platform_snowball"]
                        }
                    },
                    "filter": {
                        "platform_snowball": {
                            "type": "snowball",
                            "language": "English"
                        }
                    }
                }
            },
            mappings: {
                properties:
                {
                    "id": { "type": "keyword" },
                    "entityName": { "type": "keyword" },
                    "entityType": { "type": "keyword" },

                    "solutionId": { "type": "keyword" },
                    "organizationId": { "type": "keyword" },

                    "ownerId": { "type": "keyword" },
                    "ownerEntityName": { "type": "keyword" },
                    "ownerEntityType": { "type": "keyword" },

                    "createdBy": { "type": "keyword" },
                    "modifiedBy": { "type": "keyword" },
                    "ownedBy": { "type": "keyword" },
                    "publishedBy": { "type": "keyword" },

                    "domain": { "type": "keyword" },
                    "currency": { "type": "keyword" },

                    "country": { "type": "keyword" },
                    "city": { "type": "keyword" },
                    "language": { "type": "keyword" },
                    "type": { "type": "keyword" },

                    "createdOn": { "type": "date" },
                    "modifiedOn": { "type": "date" },
                    "ownedOn": { "type": "date" },
                    "publishedOn": { "type": "date" },

                    "tags": { "type": "text", "boost": 3 },
                    "tagsLowerCase": { "type": "text", "boost": 3 },
                    "categoryIds": { "type": "text" },
                    // "globalCategoryIds": { "type": "keyword" },

                    "isGlobal": { "type": "boolean" },
                    "isPublished": { "type": "boolean" },
                    "isSubmitted": { "type": "boolean" },
                    "isApproved": { "type": "boolean" },

                    "stateCode": { "type": "keyword" },
                    "statusCode": { "type": "keyword" },

                    "geoLocation": { "type": "geo_point" },
                    "geoShape": { "type": "geo_shape" },

                    "prevPrice": { "type": "float" },
                    "currentPrice": { "type": "float" },

                    "ipAddress": { "type": "ip" },
                    //"rank": {"type": "rank_feature"},

                    "searchDisplay": { "type": "text", "boost": 2, "analyzer": "platform_analyzer_text" },
                    "searchContent": { "type": "text", "analyzer": "platform_analyzer_text" }
                }
            }
        }
    };

    // for (var i = 0; i < nonIndexFields.length; i++) {
    //     createParam.body.mappings.properties[nonIndexFields[i].name] = { "type": "text", index: false };
    // }

    if (_.trackLibs) console.log({ name: 'creating index', createParam: JSON.stringify(createParam) });
    await searchClient.indices.create(createParam);
    if (_.trackLibs) console.log({ name: 'created index', createParam: JSON.stringify(createParam) });

    goodIndexes[indexName] = true;
};

export default { createIndex, upsertRecord, deleteRecord, hasGoodIndex, checkAndCreateIndex, search };