//  COPYRIGHT:       DouHub Inc. (C) 2021 All Right Reserved
//  COMPANY URL:     https://www.douhub.com/
//  CONTACT:         developer@douhub.com
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

'use strict';

import _ from "../../libs/helper";
import { HTTPERROR_400 } from "../../shared/libs/constants";
import CosmosDB from "../../libs/cosmos-db";
import elasticSearch from "../../libs/elastic-search";
import { getEntity } from '../../shared/libs/metadata';

//MOVED

// //When any data is created/updated/deleted by the comosDB call
// //There will be a SNS request received by this function processSNSDataToElasticSearch
// //The new data will be updated to Elastic Search
// //SNS: ${self:custom.clientName}-data
// export const processSNSDataToElasticSearch = async (event) => {

//     if (_.callFromAWSEvents(event)) return;

//     //loop through batch SNS records
//     const result = await _.processSNSRecords(event.Records, async (record) => {

//         const action = await _.getActionDataFromSNSRecord(record);
//         const cx = action.cx;
//         const context = cx && cx.context ? cx.context : {};
//         const onError = _.validateActionDataFromSNSRecord(context, event, action.data);
//         if (onError) return onError;

//         const data = action.data;


//         if (action.name == 'upsert' || action.name == 'create' || action.name == 'update') {

//             await elasticSearch.checkAndCreateIndex(cx, data.entityName, data.entityType);
//             await elasticSearch.upsert(cx, data);
//         }

//         if (action.name == 'delete') {
//             await elasticSearch.delete(cx, data);
//         }
//     });

//     if (_.track) console.log({ result });
// };


//generic query function
export const query = async (event, context, callback) => {

    const caller = await _.checkCaller(event, context, callback, { allowAnonymous: true });
    if (caller) return caller;

    const query = _.getObjectValueOfEvent(event, 'query', null);
    if (!_.isObject(query)) return _.onError(callback, { event }, HTTPERROR_400, 'ERROR_API_MISSING_PARAMETERS_QUERY', 'The parameter (query) is not provided.');

    const cx = await _.cx(event);
    try {

        //const apiToken = app.createAPIToken(cx, 'search.query');
        const searchQuery = _.cloneDeep(query);
        searchQuery.attributes = ['id'];

        //we have to make a web call to get result from the private-query end point,
        //because elastic search is running under VPC and private-query function can not access internet resource such as CosmosDB
        // const endPoint = `${process.env.APIURL}/private-query`;

        // const searchResult = await app.callAPI(endPoint, { query: searchQuery },
        //     'POST', cx.context.solution.id, apiToken);

        const searchResult = await elasticSearch.search(cx, searchQuery, false);

        if (_.track) console.log(JSON.stringify({ searchQuery, searchResult }));

        //change query to be query by ids
        delete query.pageSize;
        delete query.orderBy;
        delete query.keywords;

        const highlights = {};

        //we get all ids of the record that returned from elastic search
        query.ids = _.map(searchResult.data, (r) => {
            highlights[r.id] = r.highlight;
            return r.id;
        })

        let result = {
            _charge: 0,
            data: [],
            count: 0
        }

        if (query.ids.length>0)
        {
            //need to make a query to get all detail data from cosmosDB
            result = await CosmosDB.query(cx, query, true, true);

            result.data = _.map(result.data, (r) => {
                r.highlight = highlights[r.id];
                return r;
            })
        }
       
        return _.onSuccess(callback, cx, result);

    }
    catch (error) {
        return _.onError(callback, cx, error, `Failed to query data (${JSON.stringify(event.body)})`);
    }
};

//MOVED

// export const spellCheck = async (event, context, callback) => {

//     const text = _.getPropValueOfEvent(event, 'text');
//     if (!_.isNonEmptyString(text)) return _.onError({ event, context, callback }, HTTPERROR_400, 'ERROR_API_BAD_PARAMETERS', 'The text is not provided.');

//     const mode = _.getPropValueOfEvent(event, 'mode');
//     const mkt = _.getPropValueOfEvent(event, 'mkt');

//     const cx = await _.cx(event, {ignoreContext: true});

//     return _.onSuccess(callback, { event, context }, await _.spellCheck(cx, text, mode, mkt));
// }


export const reIndexAllData = async (event, context, callback) => {

    const cx = await _.cx(event);

    if (!_.sameGuid(cx.context.solution.ownedBy, cx.context.userId)) {
        return _.onError(callback, cx, HTTPERROR_400, 'ERROR_API_BAD_PARAMETERS', 'The caller has to be the owner of the solution.');
    }

    try {

        const entityName = _.getPropValueOfEvent(event, 'entityName');
        const pageSize = _.getIntValueOfEvent(event, 'pageSize', 100);

        const records = await CosmosDB.queryRaw(cx, {
            query: `SELECT * FROM c WHERE ${_.isNonEmptyString(entityName) ? 'c.entityName=@entityName AND ' : ''} (c.searchReindexedOn < @searchReindexedOn OR NOT IS_DEFINED(c.searchReindexedOn))`,
            parameters: [
                {
                    name: '@searchReindexedOn',
                    value: _.utcISOString(null, -60)
                },
                {
                    name: '@entityName',
                    value: entityName
                }
            ],
            pageSize
        }, true);

        const result = {};

        for (var i = 0; i < records.data.length; i++) {

            const data = records.data[i];

            try {

                if (data.entityName !== 'Domain' && data.entityName !== 'Secret') {

                    const resultProp = `${data.entityName}_${data.entityType}`;

                    const entity = getEntity(cx.context, data.entityName, data.entityType);

                    data.searchDisplay = CosmosDB.generateSearchDisplay(entity, data);
                    data.searchContent = CosmosDB.generateSearchContent(entity, data);

                    if (_.isGuid(data.ownedBy) && !data.modifiedBy) data.modifiedBy = data.ownedBy;
                    data.searchReindexedOn = _.utcISOString()

                    if (_.track) console.log(`Update ${data.id}`);
                    await CosmosDB.update(cx, data, true, 'data.update');

                    if (!result[resultProp]) result[resultProp] = 1; else result[resultProp] = result[resultProp] + 1;
                }

            }
            catch (error) {
                console.error({ error });
            }
        }

        if (_.track) console.log(result);

        return _.onSuccess(callback, cx, result);
    }
    catch (error) {
        return _.onError(callback, cx, error, `Failed to query data (${JSON.stringify(event.body)})`);
    }
};

