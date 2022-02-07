
import { isString, isArray, without, map, isNumber, each } from 'lodash';
import { HTTPERROR_403 } from 'douhub-helper-lambda';
import { isObject, isNonEmptyString, newGuid, checkEntityPrivilege } from 'douhub-helper-util';

// !!!!!!!!!!!!!!!!!!
// If you change logic here for elastic search, please remember to make same change to cosmos-db-query-processor.js


//Process query, add security conditions and generate costmosDB query format
export const processQuery = (context: Record<string, any>, req: Record<string, any>, skipSecurityCheck?:boolean) => {

    if (!isObject(context)) context = {};

    if (!isNonEmptyString(context.organizationId)) context.organizationId = newGuid();
    if (!isNonEmptyString(context.userId)) context.userId = newGuid();


    //if it has id but empty string, we will give a ramdon id, so it will return nothing
    if (isString(req.id) && req.id.trim().length == 0) req.id = newGuid();

    //if it has ids but empty array, we will give a ramdon ids, so it will return nothing
    if (isArray(req.ids) && req.ids.length == 0) req.ids = [newGuid()];


    const entityType = req.entityType;
    const entityName = req.entityName;
    let indexNames = isArray(req.indexNames) ? req.indexNames : [];

    if (!isNonEmptyString(entityName)) throw 'The entityName is not provided.';

    if (isNonEmptyString(req.entityType)) {
        indexNames.push(`${entityName}_${entityType}`);
    }
    else {
        indexNames.push(entityName);
    }

    //check basic privilege
    if (!skipSecurityCheck) {
        indexNames = without(map(indexNames, (indexName) => {
            const indexNameInfo = indexName.split('_');
            return checkEntityPrivilege(context, indexNameInfo[0], indexNameInfo[1], 'read') ? indexName : null;
        }), null);
    }

    if (indexNames.length == 0) throw HTTPERROR_403;

    //Handle the pageSize setting for the query
    //Max: 100, Default: 10
    if (!isNumber(req.pageSize)) req.pageSize = 10;
    if (req.pageSize > 100) req.pageSize = 100;


    let query: Record<string, any> = {
        index: map(indexNames, (indexName) => indexName.toLowerCase()), //Make all indexName lowercase
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
                                "stateCode": isNumber(req.stateCode) ? req.stateCode : 0
                            }
                        }]

                }
            },
            highlight: {
                require_field_match: true,
                fields: [{
                    searchDisplay: {
                        pre_tags: [
                            isNonEmptyString(req.highlightPreTag) ? req.highlightPreTag : '<span class="search-highlight">'
                        ],
                        post_tags: [
                            isNonEmptyString(req.highlightPostTag) ? req.highlightPostTag : '</span>'
                        ]
                    }
                }, {
                    searchContent: {
                        pre_tags: [
                            isNonEmptyString(req.highlightPreTag) ? req.highlightPreTag : '<span class="search-highlight">'
                        ],
                        post_tags: [
                            isNonEmptyString(req.highlightPostTag) ? req.highlightPostTag : '</span>'
                        ]
                    }
                }]
            }
        }
    };

    if (isNonEmptyString(req.aggregate)) {
        query = {
            size: 0,
            aggs: {
                list: {
                    "terms": { "field": req.aggregate, "size": 10000 }
                }
            }
        };
    }


    if (isNonEmptyString(req.keywords)) {
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
    query = handleAttributes(req, query);

    query = handleSolutionConditions( context, req, query);
    query = handleCategoryConditions(req, query);
    query = handleScopeCondition(context, req, query);

    if (!skipSecurityCheck) query = handleSecurityConditions(context, req, query);


    req.conditions = isArray(req.conditions) ? req.conditions : [];

    // req = groupConditions(req);
    query = handleOrderBy( req, query);

    return query;
};

export const groupConditions = (req: Record<string, any>) => {

    for (var i = 0; i < req.conditions.length; i++) {
        if (isObject(req.conditions[i])) {
            const paramName = `@p${newGuid().replace(/-/g, '')}`;
            const paramValue = req.conditions[i].value ? req.conditions[i].value : '';
            req.parameters.push({ name: paramName, value: paramValue });

            const attribute = isNonEmptyString(req.conditions[i].attribute) ? 'c.' + req.conditions[i].attribute : '';
            const op = isNonEmptyString(req.conditions[i].op) ? req.conditions[i].op.toUpperCase() : '';

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

export const handleCategoryConditions = (req: Record<string, any>, query: Record<string, any>) => {
    const categoryIds = req.categoryIds;
    if (!isArray(categoryIds) || isArray(categoryIds) && categoryIds.length == 0) return query;

    const terms = map(categoryIds, (categoryId) => {
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

export const handleSecurityConditions = (context: Record<string,any>,  req: Record<string, any>, query: Record<string, any>) => {
    query = handleSecurityCondition_Scope(context, req, query);
    return query;
};

export const handleSolutionConditions = (context: Record<string,any>, req: Record<string, any>, query: Record<string, any>) => {

    const { solution } = context;

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


export const handleScopeCondition = (context: Record<string,any>, req: Record<string, any>, query: Record<string, any>) => {

    req.scope = isNonEmptyString(req.scope) ? req.scope.toLowerCase() : '';

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


export const handleSecurityCondition_Scope = (context: Record<string,any>, req: Record<string, any>, query: Record<string, any>) => {

    if (req.entityName == 'Secret') return query;

    req.scope = isNonEmptyString(req.scope) ? req.scope : 'organization';
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


export const handleAttributes = (req: Record<string, any>, query: Record<string, any>) => {

    //return all fields
    if ((!isNonEmptyString(req.attributes) && !isArray(req.attributes)) || req.attributes == '*') {
        return query;
    }

    if (isNonEmptyString(req.attributes)) req.attributes = req.attributes.split(',');

    if (req.attributes.length > 0) {
        const result:Array<Record<string,any>> = [];
        for (var i = 0; i < req.attributes.length; i++) {
            result.push(req.attributes[i]);
        }
        query.body['_source'] = result.slice(0);
    }

    return query;
};

export const handleOrderBy = (req: Record<string, any>, query: Record<string, any>) => {

    if (isNonEmptyString(req.orderBy)) {
        const orderByInfo = req.orderBy.replace(/,/g, ' ').replace(/[ ]{2,}/gi, ' ').trim().split(' ');
        req.orderBy = [{ attribute: orderByInfo[0], type: orderByInfo.length <= 1 ? 'asc' : (orderByInfo.length > 1 && orderByInfo[1].toLowerCase() == 'desc' ? 'desc' : 'asc') }];
    }

    if (isArray(req.orderBy) && req.orderBy.length > 0) {

        const result:Array<Record<string,any>> = [];

        if (isNonEmptyString(req.keywords)) result.push({ "_score": { "order": "desc" } });

        // sort: [
        //     { "_score":  { "order": "desc" } },
        //     { "modifiedOn": { "order": "desc" } }]

        each(req.orderBy, (o) => {
            if (!isNonEmptyString(o.type)) o.type = 'asc';
            o.type = o.type.toLowerCase() == 'desc' ? 'desc' : 'asc';
            const orderBy: Record<string,any> = {};
            orderBy[o.attribute] = { "order": o.type };
            result.push(orderBy);
        });

        if (result.length > 0) {
            query.body.sort = result.slice(0);
        }

    }


    return query;
};
