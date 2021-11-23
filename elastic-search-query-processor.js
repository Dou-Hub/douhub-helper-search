
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
import { HTTPERROR_403 } from '../../shared/util/constants';
import { checkEntityPrivilege } from '../../shared/util/auth';
import { solution } from '../../shared/metadata/solution';

// !!!!!!!!!!!!!!!!!!
// If you change logic here for elastic search, please remember to make same change to cosmos-db-query-processor.js


//Process query, add security conditions and generate costmosDB query format
export const processQuery = (context, req, skipSecurityCheck) => {

    if (!_.isObject(context)) context = {};

    if (!_.isNonEmptyString(context.organizationId)) context.organizationId = _.newGuid();
    if (!_.isNonEmptyString(context.userId)) context.userId = _.newGuid();


    //if it has id but empty string, we will give a ramdon id, so it will return nothing
    if (_.isString(req.id) && req.id.trim().length == 0) req.id = _.newGuid();

    //if it has ids but empty array, we will give a ramdon ids, so it will return nothing
    if (_.isArray(req.ids) && req.ids.length == 0) req.ids = [_.newGuid()];


    const entityType = req.entityType;
    const entityName = req.entityName;
    let indexNames = _.isArray(req.indexNames) ? req.indexNames : [];

    if (!_.isNonEmptyString(entityName)) throw 'The entityName is not provided.';

    if (_.isNonEmptyString(req.entityType)) {
        indexNames.push(`${entityName}_${entityType}`);
    }
    else {
        indexNames.push(entityName);
    }

    //check basic privilege
    if (!skipSecurityCheck) {
        indexNames = _.without(_.map(indexNames, (indexName) => {
            const indexNameInfo = indexName.split('_');
            return checkEntityPrivilege(context, indexNameInfo[0], indexNameInfo[1], 'read') ? indexName : null;
        }), null);
    }

    if (indexNames.length == 0) throw HTTPERROR_403;

    //Handle the pageSize setting for the query
    //Max: 100, Default: 10
    if (!_.isNumber(req.pageSize)) req.pageSize = 10;
    if (req.pageSize > 100) req.pageSize = 100;


    let query = {
        index: _.map(indexNames, (indexName) => indexName.toLowerCase()), //Make all indexName lowercase
        body: {
            from: 0,
            size: req.pageSize,
            query:
            {
                bool: {
                    must: [],
                    filter:
                        [{
                            term: {
                                "stateCode": _.isNumber(req.stateCode) ? req.stateCode : 0
                            }
                        }]

                }
            },
            highlight: {
                require_field_match: true,
                fields: [{
                    searchDisplay: {
                        pre_tags: [
                            _.isNonEmptyString(req.highlightPreTag) ? req.highlightPreTag : '<span class="search-highlight">'
                        ],
                        post_tags: [
                            _.isNonEmptyString(req.highlightPostTag) ? req.highlightPostTag : '</span>'
                        ]
                    }
                }, {
                    searchContent: {
                        pre_tags: [
                            _.isNonEmptyString(req.highlightPreTag) ? req.highlightPreTag : '<span class="search-highlight">'
                        ],
                        post_tags: [
                            _.isNonEmptyString(req.highlightPostTag) ? req.highlightPostTag : '</span>'
                        ]
                    }
                }]
            }
        }
    };

    if (_.isNonEmptyString(req.aggregate)) {
        query = {
            size: 0,
            aggs: {
                list: {
                    "terms": { "field": req.aggregate, "size": 10000 }
                }
            }
        };
    }


    if (_.isNonEmptyString(req.keywords)) {
        query.body.query.bool.must.push(
            {
                multi_match:
                {
                    query: req.keywords,
                    fields: ["searchDisplay", "searchContent"]
                }
            });
    }


    //convert attribues into a comma delimited string or *
    query = handleAttributes(context, req, query);

    query = handleSolutionConditions(context, req, query);
    query = handleCategoryConditions(req, query);
    query = handleScopeCondition(context, req, query);

    if (!skipSecurityCheck) query = handleSecurityConditions(context, req, query);


    req.conditions = _.isArray(req.conditions) ? req.conditions : [];

    // req = groupConditions(req);
    query = handleOrderBy(context, req, query);

    return query;
};

export const groupConditions = (req) => {

    for (var i = 0; i < req.conditions.length; i++) {
        if (_.isObject(req.conditions[i])) {
            const paramName = `@p${_.newGuid().replace(/-/g, '')}`;
            const paramValue = req.conditions[i].value ? req.conditions[i].value : '';
            req.parameters.push({ name: paramName, value: paramValue });

            const attribute = _.isNonEmptyString(req.conditions[i].attribute) ? 'c.' + req.conditions[i].attribute : '';
            const op = _.isNonEmptyString(req.conditions[i].op) ? req.conditions[i].op.toUpperCase() : '';

            if (attribute.length > 0) {
                switch (op) {
                    case 'SEARCH':
                        {
                            req.conditions[i] = `(CONTAINS(LOWER(c.searchDisplay), ${paramName}) OR CONTAINS(LOWER(c.searchContent), ${paramName}))`;
                            break;
                        }
                    case 'CONTAINS':
                        {
                            req.conditions[i] = `${op}(${attribute}, ${paramName})`;
                            break;
                        }
                    default:
                        {
                            req.conditions[i] = `${attribute} ${op} ${paramName}`;
                            break;
                        }
                }
            }


        }
        req.query = i == 0 ? `${req.query} ${req.conditions[i]} ` : `${req.query} and (${req.conditions[i]})`;
    }

    return req;
};

export const handleCategoryConditions = (req, query) => {
    const categoryIds = req.categoryIds;
    if (!_.isArray(categoryIds) || _.isArray(categoryIds) && categoryIds.length == 0) return query;

    const terms = _.map(categoryIds, (categoryId) => {
        return { term: { categoryIds: categoryId } };
    });

    query.body.query.bool.filter.push(
        {
            bool: {
                should: terms
            }
        });

    return query;
};

export const handleSecurityConditions = (context, req, query) => {
    query = handleSecurityCondition_Scope(context, req, query);
    return query;
};

export const handleSolutionConditions = (req, query) => {

    if (
        req.entityName == 'SolutionDashboard' ||
        req.entityName == 'Site' ||
        req.entityName == 'Localization' ||
        req.entityName == 'SolutionDefinition') {

        query.body.query.bool.filter.push(
            {
                term:
                {
                    ownerId: solution.id
                }
            });
    }
    return query;
};


export const handleScopeCondition = (context, req, query) => {

    req.scope = _.isNonEmptyString(req.scope) ? req.scope.toLowerCase() : '';

    switch (req.scope) {
        case 'global':
            {
                query.body.query.bool.filter.push(
                    {
                        term:
                        {
                            isGlobal: true
                        }
                    });
                break;
            }
        case 'mine':
            {
                query.body.query.bool.filter.push(
                    {
                        term:
                        {
                            ownedBy: context.user.id
                        }
                    });
                break;
            }
        case 'global-and-mine':
            {
                query.body.query.bool.filter.push(
                    {
                        bool: {
                            should: [
                                {
                                    term:
                                    {
                                        ownedBy: context.user.id
                                    }
                                }, {
                                    term:
                                    {
                                        isGlobal: true
                                    }
                                }
                            ]
                        }
                    });
                break;
            }
        case 'organization':
            {
                query.body.query.bool.filter.push(
                    {
                        term:
                        {
                            organizationId: context.organization.id
                        }
                    });
                break;
            }
        default:
            {
                break;
            }
    }

    return query;
};


export const handleSecurityCondition_Scope = (context, req, query) => {

    if (req.entityName == 'Secret') return query;

    req.scope = _.isNonEmptyString(req.scope) ? req.scope : 'organization';
    switch (req.scope.toLowerCase()) {
        case 'global':
        case 'mine':
        case 'global-and-mine':
            {
                //has been handled by handleScopeCondition function
                break;
            }
        default: // 'organization':
            {
                query.body.query.bool.filter.push(
                    {
                        term:
                        {
                            organizationId: context.organization.id
                        }
                    });
                break;
            }

    }

    return query;
};


export const handleAttributes = (req, query) => {

    //return all fields
    if ((!_.isNonEmptyString(req.attributes) && !_.isArray(req.attributes)) || req.attributes == '*') {
        return query;
    }

    if (_.isNonEmptyString(req.attributes)) req.attributes = req.attributes.split(',');

    if (req.attributes.length > 0) {
        let result = [];
        for (var i = 0; i < req.attributes.length; i++) {
            result.push(req.attributes[i]);
        }
        query.body['_source'] = result.slice(0);
    }

    return query;
};

export const handleOrderBy = (req, query) => {

    if (_.isNonEmptyString(req.orderBy)) {
        const orderByInfo = req.orderBy.replace(/,/g, ' ').replace(/[ ]{2,}/gi, ' ').trim().split(' ');
        req.orderBy = [{ attribute: orderByInfo[0], type: orderByInfo.length <= 1 ? 'asc' : (orderByInfo.length > 1 && orderByInfo[1].toLowerCase() == 'desc' ? 'desc' : 'asc') }];
    }

    if (_.isArray(req.orderBy) && req.orderBy.length > 0) {

        let result = [];

        if (_.isNonEmptyString(req.keywords)) result.push({ "_score": { "order": "desc" } });

        // sort: [
        //     { "_score":  { "order": "desc" } },
        //     { "modifiedOn": { "order": "desc" } }]

        _.each(req.orderBy, (o) => {
            if (!_.isNonEmptyString(o.type)) o.type = 'asc';
            o.type = o.type.toLowerCase() == 'desc' ? 'desc' : 'asc';
            const orderBy = {};
            orderBy[o.attribute] = { "order": o.type };
            result.push(orderBy);
        });

        if (result.length > 0) {
            query.body.sort = result.slice(0);
        }

    }


    return query;
};
