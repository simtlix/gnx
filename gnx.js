const graphql = require('graphql')
const mongoose = require('mongoose')
mongoose.set('useFindAndModify', false)

const {
  GraphQLObjectType, GraphQLString, GraphQLID, GraphQLSchema, GraphQLList,
  GraphQLNonNull, GraphQLInputObjectType, GraphQLScalarType, Kind,
  GraphQLInt, GraphQLEnumType
} = graphql

const operations = {
  SAVE: 'save',
  UPDATE: 'update',
  DELETE: 'delete'
}

/* Schema defines data on the Graph like object types(book type), relation between
these object types and describes how it can reach into the graph to interact with
the data to retrieve or mutate the data */
const QLFilter = new GraphQLInputObjectType({
  name: 'QLFilter',
  fields: () => ({
    operator: { type: GraphQLString },
    value: { type: QLValue }
  })
})

const QLValue = new GraphQLScalarType({
  name: 'QLValue',
  serialize: parseQLValue,
  parseValue: parseQLValue,
  parseLiteral (ast) {
    if (ast.kind === Kind.INT) {
      return parseInt(ast.value, 10)
    } else if (ast.kind === Kind.FLOAT) {
      return parseFloat(ast.value)
    } else if (ast.kind === Kind.BOOLEAN) {
      return ast.value === 'true' || ast.value === true
    } else if (ast.kind === Kind.STRING) {
      return ast.value
    }
    return null
  }
})

function parseQLValue (value) {
  return value
}

const QLTypeFilter = new GraphQLInputObjectType({
  name: 'QLTypeFilter',
  fields: () => ({
    operator: { type: GraphQLString },
    value: { type: QLValue },
    path: { type: GraphQLString }
  })
})

const IdInputType = new GraphQLInputObjectType({
  name: 'IdInputType',
  fields: () => ({
    id: { type: new GraphQLNonNull(GraphQLString) }
  })
})

const QLTypeFilterExpression = new GraphQLInputObjectType({
  name: 'QLTypeFilterExpression',
  fields: () => ({
    terms: { type: new GraphQLList(QLTypeFilter) }
  })
})

const QLPagination = new GraphQLInputObjectType({
  name: 'QLPagination',
  fields: () => ({
    page: { type: new GraphQLNonNull(GraphQLInt) },
    size: { type: new GraphQLNonNull(GraphQLInt) }
  })
})

const QLSortExpression = new GraphQLInputObjectType({
  name: 'QLSortExpression',
  fields: () => ({
    terms: { type: new GraphQLList(QLSort) }
  })
})

const QLSort = new GraphQLInputObjectType({
  name: 'QLSort',
  fields: () => ({
    field: { type: new GraphQLNonNull(GraphQLString) },
    order: { type: new GraphQLNonNull(QLSortOrder) }
  })
})

const QLSortOrder = new GraphQLEnumType({
  name: 'QLSortOrder',
  values: {
    DESC: {
      value: -1
    },
    ASC: {
      value: 1
    }
  }
})

const isNonNullOfType = function (fieldEntryType, graphQLType) {
  let isOfType = false
  if (fieldEntryType instanceof GraphQLNonNull) {
    isOfType = fieldEntryType.ofType instanceof graphQLType
  }
  return isOfType
}

const buildInputType = function (model, gqltype) {
  const argTypes = gqltype.getFields()

  const fieldsArgs = {}
  const fieldsArgForUpdate = {}

  for (const fieldEntryName in argTypes) {
    const fieldEntry = argTypes[fieldEntryName]
    const fieldArg = {}
    const fieldArgForUpdate = {}

    if (fieldEntry.type instanceof GraphQLScalarType || isNonNullOfType(fieldEntry.type, GraphQLScalarType)) {
      fieldArg.type = fieldEntry.type
      fieldArgForUpdate.type = fieldEntry.type instanceof GraphQLNonNull ? fieldEntry.type.ofType : fieldEntry.type
      if (fieldEntry.type === GraphQLID) {
        fieldArgForUpdate.type = new GraphQLNonNull(GraphQLID)
      }
    } else if (fieldEntry.type instanceof GraphQLObjectType || isNonNullOfType(fieldEntry.type, GraphQLObjectType)) {
      if (fieldEntry.extensions && fieldEntry.extensions.relation) {
        if (!fieldEntry.extensions.relation.embedded) {
          fieldArg.type = fieldEntry.type instanceof GraphQLNonNull ? new GraphQLNonNull(IdInputType) : IdInputType
          fieldArgForUpdate.type = fieldArg.type
        } else if (typesDict.types[fieldEntry.type.name].inputType && typesDictForUpdate.types[fieldEntry.type.name].inputType) {
          fieldArg.type = typesDict.types[fieldEntry.type.name].inputType
          fieldArgForUpdate.type = typesDictForUpdate.types[fieldEntry.type.name].inputType
        } else {
          return null
        }
      } else {
        console.warn('Configuration issue: Field ' + fieldEntryName + ' does not define extensions.relation')
      }
    } else if (fieldEntry.type instanceof GraphQLList) {
      fieldArg.type = graphQLListInputType(typesDict, fieldEntry, fieldEntryName, 'A')
      fieldArgForUpdate.type = graphQLListInputType(typesDictForUpdate, fieldEntry, fieldEntryName, 'U')
    }

    if (fieldArg.type) {
      fieldsArgs[fieldEntryName] = fieldArg
    }

    if (fieldArgForUpdate.type) {
      fieldsArgForUpdate[fieldEntryName] = fieldArgForUpdate
    }
  }

  const inputTypeBody = {
    name: gqltype.name + 'Input',
    fields: fieldsArgs
  }

  const inputTypeBodyForUpdate = {
    name: gqltype.name + 'InputForUpdate',
    fields: fieldsArgForUpdate
  }

  return { inputTypeBody: new GraphQLInputObjectType(inputTypeBody), inputTypeBodyForUpdate: new GraphQLInputObjectType(inputTypeBodyForUpdate) }
}

const graphQLListInputType = function (dict, fieldEntry, fieldEntryName, inputNamePrefix) {
  const ofType = fieldEntry.type.ofType

  if (ofType instanceof GraphQLObjectType && dict.types[ofType.name].inputType) {
    if (!fieldEntry.extensions || !fieldEntry.extensions.relation || !fieldEntry.extensions.relation.embedded) {
      const oneToMany = new GraphQLInputObjectType({
        name: 'OneToMany' + inputNamePrefix + fieldEntryName,
        fields: () => ({
          added: { type: new GraphQLList(dict.types[ofType.name].inputType) },
          updated: { type: new GraphQLList(dict.types[ofType.name].inputType) },
          deleted: { type: new GraphQLList(dict.types[ofType.name].inputType) }
        })
      })

      return oneToMany
    } else if (fieldEntry.extensions && fieldEntry.extensions.relation && fieldEntry.extensions.relation.embedded) {
      return new GraphQLList(dict.types[ofType.name].inputType)
    }
  } else if (ofType instanceof GraphQLScalarType) {
    return new GraphQLList(ofType)
  } else {
    return null
  }
}

const buildPendingInputTypes = function (waitingInputType) {
  const stillWaitingInputType = {}
  let isThereAtLeastOneWaiting = false

  for (const pendingInputTypeName in waitingInputType) {
    const model = waitingInputType[pendingInputTypeName].model
    const gqltype = waitingInputType[pendingInputTypeName].gqltype

    const { inputTypeBody, inputTypeBodyForUpdate } = buildInputType(model, gqltype)

    if (inputTypeBody && inputTypeBodyForUpdate) {
      typesDict.types[gqltype.name].inputType = inputTypeBody
      typesDictForUpdate.types[gqltype.name].inputType = inputTypeBodyForUpdate
    } else {
      stillWaitingInputType[pendingInputTypeName] = waitingInputType[pendingInputTypeName]
      isThereAtLeastOneWaiting = true
    }
  }

  if (isThereAtLeastOneWaiting) {
    buildPendingInputTypes(stillWaitingInputType)
  }
}

const buildRootQuery = function (name) {
  const rootQueryArgs = {}
  rootQueryArgs.name = name
  rootQueryArgs.fields = {}

  for (const entry in typesDict.types) {
    const type = typesDict.types[entry]

    rootQueryArgs.fields[type.simpleEntityEndpointName] = {
      type: type.gqltype,
      args: { id: { type: GraphQLID } },
      resolve (parent, args) {
        /* Here we define how to get data from database source
        this will return the type with id passed in argument
        by the user */
        return type.model.findById(args.id)
      }
    }

    const argTypes = type.gqltype.getFields()

    const argsObject = {}

    for (const fieldEntryName in argTypes) {
      const fieldEntry = argTypes[fieldEntryName]
      argsObject[fieldEntryName] = {}

      if (fieldEntry.type instanceof GraphQLScalarType || isNonNullOfType(fieldEntry.type, GraphQLScalarType)) {
        argsObject[fieldEntryName].type = QLFilter
      } else if (fieldEntry.type instanceof GraphQLObjectType || fieldEntry.type instanceof GraphQLList || isNonNullOfType(fieldEntry.type, GraphQLObjectType)) {
        argsObject[fieldEntryName].type = QLTypeFilterExpression
      }
    }

    argsObject.pagination = {}
    argsObject.pagination.type = QLPagination

    argsObject.sort = {}
    argsObject.sort.type = QLSortExpression

    rootQueryArgs.fields[type.listEntitiesEndpointName] = {
      type: new GraphQLList(type.gqltype),
      args: argsObject,
      async resolve (parent, args) {
        const aggregateClauses = await buildQuery(args, type.gqltype)
        let result
        if (aggregateClauses.length === 0) {
          result = type.model.find({})
        } else {
          result = type.model.aggregate(aggregateClauses)
        }
        return result
      }
    }
  }

  return new GraphQLObjectType(rootQueryArgs)
}

const materializeModel = function (args, gqltype, linkToParent) {
  if (!args) {
    return null
  }

  const argTypes = gqltype.getFields()

  const modelArgs = {}
  const collectionFields = {}

  for (const fieldEntryName in argTypes) {
    const fieldEntry = argTypes[fieldEntryName]

    if (!args[fieldEntryName]) {
      continue
    }

    if (fieldEntry.type instanceof GraphQLScalarType || isNonNullOfType(fieldEntry.type, GraphQLScalarType)) {
      modelArgs[fieldEntryName] = args[fieldEntryName]
    } else if (fieldEntry.type instanceof GraphQLObjectType || isNonNullOfType(fieldEntry.type, GraphQLObjectType)) {
      if (fieldEntry.extensions && fieldEntry.extensions.relation) {
        if (!fieldEntry.extensions.relation.embedded) {
          modelArgs[fieldEntry.extensions.relation.connectionField] = new mongoose.Types.ObjectId(args[fieldEntryName].id)
        } else {
          modelArgs[fieldEntryName] = materializeModel(args[fieldEntryName], fieldEntry.type).modelArgs
        }
      } else {
        console.warn('Configuration issue: Field ' + fieldEntryName + ' does not define extensions.relation')
      }
    } else if (fieldEntry.type instanceof GraphQLList) {
      const ofType = fieldEntry.type.ofType
      if (ofType instanceof GraphQLObjectType && fieldEntry.extensions && fieldEntry.extensions.relation) {
        if (!fieldEntry.extensions.relation.embedded) {
          collectionFields[fieldEntryName] = args[fieldEntryName]
        } else if (fieldEntry.extensions.relation.embedded) {
          const collectionEntries = []

          args[fieldEntryName].forEach(element => {
            const collectionEntry = materializeModel(element, ofType).modelArgs
            if (collectionEntry) {
              collectionEntries.push(collectionEntry)
            }
          })

          modelArgs[fieldEntryName] = collectionEntries
        }
      } else if (ofType instanceof GraphQLScalarType) {
        modelArgs[fieldEntryName] = args[fieldEntryName]
      }
    }
  }
  if (linkToParent) {
    linkToParent(modelArgs)
  }

  return { modelArgs: modelArgs, collectionFields: collectionFields }
}

const executeOperation = async function (Model, gqltype, args, operation) {
  const session = await mongoose.startSession()
  await session.startTransaction()
  try {
    let newObject = null
    switch (operation) {
      case operations.SAVE:
        newObject = await onSaveObject(Model, gqltype, args, session)
        break
      case operations.UPDATE:
        newObject = await onUpdateSubject(Model, gqltype, args, session)
        break
      case operations.DELETE:
        newObject = await onDeleteObject(Model, gqltype, args, session)
        break
    }
    console.log('before transaction')
    await session.commitTransaction()
    return newObject
  } catch (error) {
    await session.abortTransaction()
    throw error
  } finally {
    session.endSession()
  }
}

const onDeleteObject = async function (Model, gqltype, args, session, linkToParent) {
  const result = materializeModel(args, gqltype, linkToParent)
  const deletedObject = new Model(result.modelArgs)
  return Model.findByIdAndDelete(args, deletedObject.modelArgs).session(session)
}

const onUpdateSubject = async function (Model, gqltype, args, session, linkToParent) {
  const materializedModel = materializeModel(args, gqltype, linkToParent)
  const objectId = args.id

  if (materializedModel.collectionFields) {
    iterateonCollectionFields(materializeModel, gqltype, objectId, session)
  }

  let modifiedObject = materializedModel.modelArgs
  const currentObject = await Model.findById({ _id: objectId })

  const argTypes = gqltype.getFields()
  for (const fieldEntryName in argTypes) {
    const fieldEntry = argTypes[fieldEntryName]
    if (fieldEntry.extensions && fieldEntry.extensions.relation && fieldEntry.extensions.relation.embedded) {
      const oldObjectData = currentObject[fieldEntryName]
      const newObjectData = modifiedObject[fieldEntryName]
      if (newObjectData) {
        if (Array.isArray(oldObjectData) && Array.isArray(newObjectData)) {
          modifiedObject[fieldEntryName] = newObjectData
        } else {
          modifiedObject[fieldEntryName] = { ...oldObjectData, ...newObjectData }
        }
      }
    }

    if (args[fieldEntryName] === null && !(argTypes[fieldEntryName].type instanceof GraphQLNonNull)) {
      modifiedObject = { ...modifiedObject, $unset: { [fieldEntryName]: '' } }
    }
  }

  return Model.findByIdAndUpdate(
    objectId, modifiedObject, { new: true }
  )
}

const onSaveObject = async function (Model, gqltype, args, session, linkToParent) {
  const materializedModel = materializeModel(args, gqltype, linkToParent)
  const newObject = new Model(materializedModel.modelArgs)
  console.log(JSON.stringify(newObject))
  newObject.$session(session)

  if (materializedModel.collectionFields) {
    iterateonCollectionFields(materializedModel, gqltype, newObject._id, session)
  }

  return newObject.save()
}

const iterateonCollectionFields = function (materializedModel, gqltype, objectId, session) {
  for (const collectionField in materializedModel.collectionFields) {
    if (materializedModel.collectionFields[collectionField].added) {
      executeItemFunction(gqltype, collectionField, objectId, session, materializedModel.collectionFields[collectionField].added, operations.SAVE)
    }
    if (materializedModel.collectionFields[collectionField].updated) {
      executeItemFunction(gqltype, collectionField, objectId, session, materializedModel.collectionFields[collectionField].updated, operations.UPDATE)
    }
    if (materializedModel.collectionFields[collectionField].deleted) {
      executeItemFunction(gqltype, collectionField, objectId, session, materializedModel.collectionFields[collectionField].updated, operations.DELETE)
    }
  }
}

const executeItemFunction = function (gqltype, collectionField, objectId, session, collectionFieldsList, operationType) {
  const argTypes = gqltype.getFields()
  const collectionGQLType = argTypes[collectionField].type.ofType
  const connectionField = argTypes[collectionField].extensions.relation.connectionField

  let operationFunction = function () {}

  switch (operationType) {
    case operations.SAVE:
      operationFunction = collectionItem => {
        onSaveObject(typesDict.types[collectionGQLType.name].model, collectionGQLType, collectionItem, session, (item) => {
          item[connectionField] = objectId
        })
      }
      break
    case operations.UPDATE:
      operationFunction = collectionItem => {
        onUpdateSubject(typesDict.types[collectionGQLType.name].model, collectionGQLType, collectionItem, session, (item) => {
          item[connectionField] = objectId
        })
      }
      break
    case operations.DELETE:
    // TODO: implement
  }

  collectionFieldsList.forEach(collectionItem => {
    operationFunction(collectionItem)
  })
}

const buildMutation = function (name) {
  const rootQueryArgs = {}
  rootQueryArgs.name = name
  rootQueryArgs.fields = {}

  buildPendingInputTypes(waitingInputType)

  for (const entry in typesDict.types) {
    const type = typesDict.types[entry]

    if (type.endpoint) {
      const argsObject = { input: { type: new GraphQLNonNull(type.inputType) } }

      rootQueryArgs.fields['add' + type.simpleEntityEndpointName] = {
        type: type.gqltype,
        args: argsObject,
        async resolve (parent, args) {
          return executeOperation(type.model, type.gqltype, args.input, operations.SAVE)
        }
      }
      rootQueryArgs.fields['delete' + type.simpleEntityEndpointName] = {
        type: type.gqltype,
        args: { id: { type: new GraphQLNonNull(GraphQLID) } },
        async resolve (parent, args) {
          return executeOperation(type.model, type.gqltype, args.id, operations.DELETE)
        }
      }
    }
  }

  for (const entry in typesDictForUpdate.types) {
    const type = typesDictForUpdate.types[entry]

    if (type.endpoint) {
      const argsObject = { input: { type: new GraphQLNonNull(type.inputType) } }
      rootQueryArgs.fields['update' + type.simpleEntityEndpointName] = {
        type: type.gqltype,
        args: argsObject,
        async resolve (parent, args) {
          return executeOperation(type.model, type.gqltype, args.input, operations.UPDATE)
        }
      }
    }
  }

  return new GraphQLObjectType(rootQueryArgs)
}

const typesDict = { types: {} }
const waitingInputType = {}
const typesDictForUpdate = { types: {} }

/* Creating a new GraphQL Schema, with options query which defines query
we will allow users to use when they are making request. */
module.exports.createSchema = function () {
  return new GraphQLSchema({
    query: buildRootQuery('RootQueryType'),
    mutation: buildMutation('Mutation')
  })
}

module.exports.connect = function (model, gqltype, simpleEntityEndpointName, listEntitiesEndpointName) {
  waitingInputType[gqltype.name] = {
    model: model,
    gqltype: gqltype
  }

  typesDict.types[gqltype.name] = {
    model: model,
    gqltype: gqltype,
    simpleEntityEndpointName: simpleEntityEndpointName,
    listEntitiesEndpointName: listEntitiesEndpointName,
    endpoint: true
  }

  typesDictForUpdate.types[gqltype.name] = { ...typesDict.types[gqltype.name] }
}

module.exports.addNoEndpointType = function (gqltype) {
  waitingInputType[gqltype.name] = {
    gqltype: gqltype
  }

  typesDict.types[gqltype.name] = {
    gqltype: gqltype,
    endpoint: false
  }

  typesDictForUpdate.types[gqltype.name] = { ...typesDict.types[gqltype.name] }
}

const buildQuery = async function (input, gqltype) {
  console.log('Building Query')
  const aggregateClauses = []
  const matchesClauses = { $match: {} }
  let addMatch = false
  let limitClause = {}
  let skipClause = {}
  let addPagination = false
  let sortClause = {}
  let addSort = false

  for (const key in input) {
    if (Object.prototype.hasOwnProperty.call(input, key) && key !== 'pagination' && key !== 'sort') {
      const filterField = input[key]
      const qlField = gqltype.getFields()[key]

      const result = await buildQueryTerms(filterField, qlField, key)

      if (result) {
        for (const aggregate in result.aggregateClauses) {
          aggregateClauses.push(result.aggregateClauses[aggregate].lookup)
          aggregateClauses.push(result.aggregateClauses[aggregate].unwind)
        }

        for (const match in result.matchesClauses) {
          if (Object.prototype.hasOwnProperty.call(result.matchesClauses, match)) {
            const matchClause = result.matchesClauses[match]
            for (const key in matchClause) {
              if (Object.prototype.hasOwnProperty.call(matchClause, key)) {
                const value = matchClause[key]
                matchesClauses.$match[key] = value
                addMatch = true
              }
            }
          }
        }
      }
    } else if (key === 'pagination') {
      if (input[key].page && input[key].size) {
        const skip = input[key].size * (input[key].page - 1)
        limitClause = { $limit: input[key].size + skip }
        skipClause = { $skip: skip }
        addPagination = true
      }
    } else if (key === 'sort') {
      const sortExpressions = {}
      input[key].terms.forEach(function (sort) {
        console.log(sort)
        sortExpressions[sort.field] = sort.order
      })
      sortClause = { $sort: sortExpressions }
      addSort = true
    }
  }

  if (addMatch) {
    aggregateClauses.push(matchesClauses)
  }

  if (addSort) {
    aggregateClauses.push(sortClause)
  }

  if (addPagination) {
    aggregateClauses.push(limitClause)
    aggregateClauses.push(skipClause)
  }

  console.log(JSON.stringify(aggregateClauses))
  return aggregateClauses
}

const buildQueryTerms = async function (filterField, qlField, fieldName) {
  const aggregateClauses = {}
  const matchesClauses = {}

  if (qlField.type instanceof GraphQLScalarType || isNonNullOfType(qlField.type, GraphQLScalarType)) {
    const matchesClause = {}
    // TODO only equal for now
    matchesClause[fieldName] = filterField.value
    matchesClauses[fieldName] = matchesClause
  } else if (qlField.type instanceof GraphQLObjectType || qlField.type instanceof GraphQLList || isNonNullOfType(qlField.type, GraphQLObjectType)) {
    let fieldType = qlField.type

    if (fieldType instanceof GraphQLList || fieldType instanceof GraphQLNonNull) {
      fieldType = qlField.type.ofType
    }

    filterField.terms.forEach(term => {
      const model = typesDict.types[fieldType.name].model
      const collectionName = model.collection.collectionName
      const localFieldName = qlField.extensions.relation.connectionField

      if (qlField.extensions && qlField.extensions.relation && !qlField.extensions.relation.embedded) {
        if (!aggregateClauses[fieldName]) {
          let lookup = {}

          if (qlField.type instanceof GraphQLList) {
            lookup = {
              $lookup: {
                from: collectionName,
                foreignField: localFieldName,
                localField: '_id',
                as: fieldName
              }
            }
          } else {
            lookup = {
              $lookup: {
                from: collectionName,
                foreignField: '_id',
                localField: localFieldName,
                as: fieldName
              }
            }
          }

          aggregateClauses[fieldName] = {
            lookup: lookup,
            unwind: { $unwind: { path: '$' + fieldName, preserveNullAndEmptyArrays: true } }
          }
        }
      }

      if (term.path.indexOf('.') < 0) {
        const matchesClause = {}
        matchesClause[fieldName + '.' + term.path] = term.value
        matchesClauses[fieldName] = matchesClause
      } else {
        let currentGQLPathFieldType = qlField.type
        let aliasPath = fieldName
        let embeddedPath = ''

        term.path.split('.').forEach((pathFieldName) => {
          const pathField = currentGQLPathFieldType.getFields()[pathFieldName]
          if (pathField.type instanceof GraphQLScalarType || isNonNullOfType(pathField.type, GraphQLScalarType)) {
            const matchesClause = {}
            matchesClause[aliasPath + (embeddedPath !== '' ? '.' + embeddedPath + '.' : '.') + pathFieldName] = term.value
            matchesClauses[aliasPath + '_' + pathFieldName] = matchesClause
            embeddedPath = ''
          } else if (pathField.type instanceof GraphQLObjectType || pathField.type instanceof GraphQLList || isNonNullOfType(pathField.type, GraphQLObjectType)) {
            let pathFieldType = pathField.type
            if (pathField.type instanceof GraphQLList || pathField.type instanceof GraphQLNonNull) {
              pathFieldType = pathField.type.ofType
            }
            currentGQLPathFieldType = pathFieldType

            if (pathField.extensions && pathField.extensions.relation && !pathField.extensions.relation.embedded) {
              const currentPath = aliasPath + (embeddedPath !== '' ? '.' + embeddedPath : '')
              aliasPath += (embeddedPath !== '' ? '_' + embeddedPath + '_' : '_') + pathFieldName

              embeddedPath = ''

              const pathModel = typesDict.types[pathFieldType.name].model
              const fieldPathCollectionName = pathModel.collection.collectionName
              const pathLocalFieldName = pathField.extensions.relation.connectionField

              let lookup = {}
              if (!aggregateClauses[aliasPath]) {
                if (pathField.type instanceof GraphQLList) {
                  lookup = {
                    $lookup: {
                      from: fieldPathCollectionName,
                      foreignField: pathLocalFieldName,
                      localField: currentPath + '.' + '_id',
                      as: aliasPath
                    }
                  }
                } else {
                  lookup = {
                    $lookup: {
                      from: fieldPathCollectionName,
                      foreignField: '_id',
                      localField: currentPath + '.' + pathLocalFieldName,
                      as: aliasPath
                    }
                  }
                }
              } else {
                lookup = {
                  $lookup: {
                    from: fieldPathCollectionName,
                    foreignField: '_id',
                    localField: currentPath + '.' + pathLocalFieldName,
                    as: aliasPath
                  }
                }
              }

              aggregateClauses[aliasPath] = {
                lookup: lookup,
                unwind: { $unwind: { path: '$' + aliasPath, preserveNullAndEmptyArrays: true } }
              }
            }
          } else {
            if (embeddedPath === '') {
              embeddedPath += pathFieldName
            } else {
              embeddedPath += '.' + pathFieldName
            }
          }
        })
      }
    })
  }
  return { aggregateClauses: aggregateClauses, matchesClauses: matchesClauses }
}
