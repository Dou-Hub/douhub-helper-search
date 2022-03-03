
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

import { map, cloneDeep, each, orderBy } from 'lodash';
import { _track, isObject, isNonEmptyString, _process, getEntity } from 'douhub-helper-util';
import { processQuery } from './elastic-search-query-processor';
import { getSolution } from 'douhub-helper-lambda';
import { elasticSearchQuery, elasticSearchDelete, elasticSearchUpsert, getElasticSearch, cosmosDBRetrieveByIds } from 'douhub-helper-service';

export const queryRecords = async (
    context: Record<string, any>,
    query: Record<string, any>,
    skipSecurityCheck?: boolean,
    settings?: {
        includeRawRecord?: boolean,
        attributes?: string
    }) => {


    let result: Record<string, any> = {};

    const includeRawRecord = settings?.includeRawRecord ? true : false;
    const attributes = settings?.attributes ? settings?.attributes : '';
    const solutionId = context.solutionId;

    await checkAndCreateIndex(solutionId, query.entityName, query.entityType);

    if (_track) console.log({ queryPreProcess: JSON.stringify(query) });

    query = processQuery(context, query, skipSecurityCheck);

    if (_track) console.log({ queryPostProcess: JSON.stringify(query) });


    result = await elasticSearchQuery(query);

    const data = result.hits.hits;
    const total = result.hits.total.value;

    const ids: string[] = [];
    const finder: Record<string, any> = {};
    result = { total };

    result.data = map(data, (r) => {
        const s = r['_source'];
        s.highlight = r.highlight;
        s.score = r['_score'];
        if (includeRawRecord) {
            ids.push(s.id);
            finder[s.id] = {
                highlight: s.highlight,
                score: s.score
            };
        }
        return s;
    });

    if (includeRawRecord && ids.length > 0) {

        //need to make a query to get all detail data from cosmosDB
        const records = await cosmosDBRetrieveByIds(ids, { attributes, includeAzureInfo: false });

        result.data = orderBy(map(records, (r) => {
            const { highlight, score } = finder[r.id];
            return { ...r, highlight, score };
        }), ['score'], ['desc']);
    }

    return result;

};

//upsert will have no permission check, it is simply a base function to be called with fully trust
export const upsertRecord = async (rawData: Record<string, any>) => {

    const data = cloneDeep(rawData);
    const entityName = data.entityName;
    const entityType = data.entityType;
    const id = data.id;

    if (!isNonEmptyString(entityName)) throw 'The entityName is not provided.';
    if (!isNonEmptyString(id)) throw 'The id is not provided.';

    if (_track) console.log({ name: 'elastic-search-upsert', data: JSON.stringify(data) });

    //need to clean up some fields that will messup elastic search
    delete data['_rid'];
    delete data['_attachments'];
    delete data['_self'];
    delete data['_etag'];
    delete data['_ts'];

    //The fields that has been merged into the searchDisplay and searchContent does not need to be kept
    each([
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

    await checkAndCreateIndex(data.solutionId, data.entityName, data.entityType);

    if (_track) console.log({ name: 'elastic-search-upsert', data: JSON.stringify(data) });

    //we will always have an index at entityName level
    await elasticSearchUpsert(entityName.toLowerCase(), data);

    //if there entityType, we will also index the record in entityType index
    if (isNonEmptyString(entityType)) {
        await elasticSearchUpsert(`${entityName}_${entityType}`.toLowerCase(), data);
    }

    return data;

};

//upsert will have no permission check, it is simply a base function to be called with fully trust
export const deleteRecord = async (data: Record<string, any>) => {

    const entityName = data.entityName;
    const entityType = data.entityType;
    const id = data.id;

    if (!isNonEmptyString(entityName)) throw 'The entityName is not provided.';
    if (!isNonEmptyString(id)) throw 'The id is not provided.';

    if (_track) console.log({ data: JSON.stringify(data) });

    //we will always have an index at entityName level
    await elasticSearchDelete(entityName.toLowerCase(), id);

    //if there entityType, we will also index the record in entityType index
    await elasticSearchDelete(`${entityName}_${entityType}`.toLowerCase(), id);

    return data;

};

export const checkAndCreateIndex = async (solutionId: string, entityName: string, entityType: string, forceCreate?: boolean) => {

    if (!isNonEmptyString(entityName)) throw 'The entityName is not provided.';

    const entityNameIndexName = entityName.toLowerCase();
    const entityTypeIndexName = `${entityName}_${entityType}`.toLowerCase();

    if (forceCreate || !await hasGoodIndex(entityNameIndexName)) {
        if (_track) console.log(`checkAndCreateIndex - create index - ${entityNameIndexName}`);
        await createIndex(solutionId, entityNameIndexName);
    }

    if (isNonEmptyString(entityType) && (forceCreate || !await hasGoodIndex(entityTypeIndexName))) {
        if (_track) console.log(`checkAndCreateIndex - create index - ${entityTypeIndexName}`);
        await createIndex(solutionId, entityName, entityType);
    }
};

export const hasGoodIndex = async (indexName: string) => {

    if (!_process._goodIndexes) _process._goodIndexes = {};
    if (_process._goodIndexes[indexName]) {
        if (_track) console.log(`hasGoodIndex - ${indexName} - from cache`);
        return true;
    }

    const searchClient = await getElasticSearch();
    let result = true;
    try {
        const mappings = (await searchClient.indices.getMapping({ index: indexName })).body[indexName].mappings;
        if (_track) console.log(`hasGoodIndex - ${indexName} - get mapping`);
        if (mappings.properties.id.type !== 'keyword') {
            result = false;
        }
    }
    catch (error) {
        console.error({ error: isObject(error) ? JSON.stringify(error) : error });
        result = false;
    }

    if (!result) {
        if (_track) console.log(`hasGoodIndex - ${indexName} - index does not exist`);
    }
    else {
        _process._goodIndexes[indexName] = true;
    }

    return result;
};

export const createIndex = async (solutionId: string, entityName: string, entityType?: string) => {
    //We will need to make some fields not analyzed so we can use exact match when query
    //To prevent this from happening, we need to tell elastic that it is an exact value and it shouldn’t be analyzed to split into tokens.

    const indexName = isNonEmptyString(entityType) ? `${entityName}_${entityType}`.toLowerCase() : entityName.toLowerCase();

    //get entity definition from solutio file
    const solution = await getSolution(solutionId);
    const entity = getEntity(solution, entityName, entityType)

    const searchClient = await getElasticSearch();
    if (_track) console.log({ name: 'elastic-search-checking-exist-index', indexName });
    let indexExists = await searchClient.indices.exists({ index: indexName });
    if (_track) console.log({ name: 'elastic-search-checked-exist-index', indexName, indexExists });
    if (isObject(indexExists)) indexExists = indexExists.body;

    const addtionalProperties = entity && isObject(entity.searchProperties) ? entity.searchProperties : {};

    //delete first
    if (indexExists) {
        //force recreate, we will need delete first
        if (_track) console.log({ name: 'elastic-search-deleting-index', indexName });
        await searchClient.indices.delete({ index: indexName });
        if (_track) console.log({ name: 'elastic-search-deleted-index', indexName });
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

                    "slug": { "type": "keyword" },

                    "createdOn": { "type": "date" },
                    "modifiedOn": { "type": "date" },
                    "ownedOn": { "type": "date" },
                    "publishedOn": { "type": "date" },

                    "tags": { "type": "text", "boost": 3 },
                    "tagsLowerCase": { "type": "text", "boost": 3 },
                    "categoryIds": { "type": "text" },
                    "slugs": { "type": "text" },
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
                    "searchContent": { "type": "text", "analyzer": "platform_analyzer_text" },
                    ...addtionalProperties
                }
            }
        }
    };

    // for (var i = 0; i < nonIndexFields.length; i++) {
    //     createParam.body.mappings.properties[nonIndexFields[i].name] = { "type": "text", index: false };
    // }

    if (_track) console.log({ name: 'creating index', createParam: JSON.stringify(createParam) });
    await searchClient.indices.create(createParam);
    if (_track) console.log({ name: 'created index', createParam: JSON.stringify(createParam) });

    _process._goodIndexes[indexName] = true;
};
