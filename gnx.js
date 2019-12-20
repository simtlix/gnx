const graphql = require('graphql')
const mongoose = require('mongoose')
mongoose.set('useFindAndModify', false)

const {
  GraphQLObjectType, GraphQLString, GraphQLID, GraphQLSchema, GraphQLList,
  GraphQLNonNull, GraphQLInputObjectType, GraphQLScalarType, Kind,
  GraphQLInt, GraphQLEnumType, GraphQLBoolean, GraphQLFloat
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
    operator: { type: QLOperator },
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
    } else if (ast.kind === Kind.LIST) {
      const values = []
      ast.values.forEach(value => {
        if (value.kind === Kind.INT) {
          values.push(parseInt(value.value, 10))
        } else if (value.kind === Kind.FLOAT) {
          values.push(parseFloat(value.value))
        } else if (value.kind === Kind.BOOLEAN) {
          values.push(value.value === 'true' || value.value === true)
        } else if (value.kind === Kind.STRING) {
          values.push(value.value)
        }
      })
      return values
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
    operator: { type: QLOperator },
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

const QLOperator = new GraphQLEnumType({
  name: 'QLOperator',
  values: {
    EQ: {
      value: 0
    },
    LT: {
      value: 1
    },
    GT: {
      value: 2
    },
    LTE: {
      value: 3
    },
    GTE: {
      value: 4
    },
    BTW: {
      value: 5
    },
    NE: {
      value: 6
    },
    IN: {
      value: 7
    },
    NIN: {
      value: 8
    },
    LIKE: {
      value: 9
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

const isNonNullOfTypeForNotScalar = function (fieldEntryType, graphQLType) {
  let isOfType = false
  if (fieldEntryType instanceof GraphQLNonNull) {
    isOfType = fieldEntryType.ofType === graphQLType
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

    if (fieldEntry.extensions && fieldEntry.extensions.readOnly) {
      continue
    }

    if (fieldEntry.type instanceof GraphQLScalarType || fieldEntry.type instanceof GraphQLEnumType ||
        isNonNullOfType(fieldEntry.type, GraphQLScalarType) || isNonNullOfType(fieldEntry.type, GraphQLEnumType)
    ) {
      if (fieldEntryName != 'id') {
        fieldArg.type = fieldEntry.type
      }
      fieldArgForUpdate.type = fieldEntry.type instanceof GraphQLNonNull ? fieldEntry.type.ofType : fieldEntry.type
      if (fieldEntry.type === GraphQLID) {
        fieldArgForUpdate.type = new GraphQLNonNull(GraphQLID)
      }
    } else if (fieldEntry.type instanceof GraphQLObjectType || isNonNullOfType(fieldEntry.type, GraphQLObjectType)) {
      if (fieldEntry.extensions && fieldEntry.extensions.relation) {
        const fieldEntryName = fieldEntry.type instanceof GraphQLNonNull ? fieldEntry.type.ofType.name : fieldEntry.type.name
        if (!fieldEntry.extensions.relation.embedded) {
          fieldArg.type = fieldEntry.type instanceof GraphQLNonNull ? new GraphQLNonNull(IdInputType) : IdInputType
          fieldArgForUpdate.type = IdInputType
        } else if (typesDict.types[fieldEntryName].inputType && typesDictForUpdate.types[fieldEntryName].inputType) {
          fieldArg.type = typesDict.types[fieldEntryName].inputType
          fieldArgForUpdate.type = typesDictForUpdate.types[fieldEntryName].inputType
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
          added: { type: new GraphQLList(typesDict.types[ofType.name].inputType) },
          updated: { type: new GraphQLList(typesDictForUpdate.types[ofType.name].inputType) },
          deleted: { type: new GraphQLList(typesDict.types[ofType.name].inputType) }
        })
      })

      return oneToMany
    } else if (fieldEntry.extensions && fieldEntry.extensions.relation && fieldEntry.extensions.relation.embedded) {
      return new GraphQLList(dict.types[ofType.name].inputType)
    }
  } else if (ofType instanceof GraphQLScalarType || ofType instanceof GraphQLEnumType) {
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

      //Fixing resolve method in order to be compliant with Mongo _id field
    if(type.gqltype.getFields()["id"] && !type.gqltype.getFields()["id"].resolve){
      type.gqltype.getFields()["id"].resolve = function(parent) {return parent._id}
    }

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

      if (fieldEntry.type instanceof GraphQLScalarType || isNonNullOfType(fieldEntry.type, GraphQLScalarType) ||
          fieldEntry.type instanceof GraphQLEnumType || isNonNullOfType(fieldEntry.type, GraphQLEnumType)) {
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

const isEmpty = function (value) {
  return !value && value !== false
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

    if (isEmpty(args[fieldEntryName])) {
      continue
    }

    if (fieldEntry.type instanceof GraphQLScalarType || isNonNullOfType(fieldEntry.type, GraphQLScalarType)) {
      modelArgs[fieldEntryName] = args[fieldEntryName]
    } else if (fieldEntry.type instanceof GraphQLObjectType || isNonNullOfType(fieldEntry.type, GraphQLObjectType)) {
      if (fieldEntry.extensions && fieldEntry.extensions.relation) {
        if (!fieldEntry.extensions.relation.embedded) {
          modelArgs[fieldEntry.extensions.relation.connectionField] = new mongoose.Types.ObjectId(args[fieldEntryName].id)
        } else {
          const fieldType = fieldEntry.type instanceof GraphQLNonNull ? fieldEntry.type.ofType : fieldEntry.type
          modelArgs[fieldEntryName] = materializeModel(args[fieldEntryName], fieldType).modelArgs
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
      } else if (ofType instanceof GraphQLScalarType || ofType instanceof GraphQLEnumType) {
        modelArgs[fieldEntryName] = args[fieldEntryName]
      }
    }
  }
  if (linkToParent) {
    linkToParent(modelArgs)
  }

  return { modelArgs: modelArgs, collectionFields: collectionFields }
}

const executeOperation = async function (Model, gqltype, controller, args, operation) {
  const session = await mongoose.startSession()
  await session.startTransaction()
  try {
    let newObject = null
    switch (operation) {
      case operations.SAVE:
        newObject = await onSaveObject(Model, gqltype, controller, args, session)
        break
      case operations.UPDATE:
        newObject = await onUpdateSubject(Model, gqltype, controller, args, session)
        break
      case operations.DELETE:
        newObject = await onDeleteObject(Model, gqltype, controller, args, session)
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

const onDeleteObject = async function (Model, gqltype, controller, args, session, linkToParent) {
  const result = materializeModel(args, gqltype, linkToParent)
  const deletedObject = new Model(result.modelArgs)

  if (controller && controller.onDelete) {
    await controller.onDelete(deletedObject)
  }

  return Model.findByIdAndDelete(args, deletedObject.modelArgs).session(session)
}

const onUpdateSubject = async function (Model, gqltype, controller, args, session, linkToParent) {
  const materializedModel = materializeModel(args, gqltype, linkToParent)
  const objectId = args.id

  if (materializedModel.collectionFields) {
    iterateonCollectionFields(materializedModel, gqltype, objectId, session)
  }

  let modifiedObject = materializedModel.modelArgs
  const currentObject = await Model.findById({ _id: objectId }).lean()

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

  if (controller && controller.onUpdating) {
    controller.onUpdating(objectId, modifiedObject)
  }

  const result = Model.findByIdAndUpdate(
    objectId, modifiedObject, { new: true }
  )

  if (controller && controller.onUpdated) {
    controller.onUpdated(result)
  }

  return result
}

const onSaveObject = async function (Model, gqltype, controller, args, session, linkToParent) {
  const materializedModel = materializeModel(args, gqltype, linkToParent)
  const newObject = new Model(materializedModel.modelArgs)
  console.log(JSON.stringify(newObject))
  newObject.$session(session)

  if (controller && controller.onSaving) {
    controller.onSaving(newObject)
  }

  if (materializedModel.collectionFields) {
    iterateonCollectionFields(materializedModel, gqltype, newObject._id, session)
  }

  const result = newObject.save()
  if (controller && controller.onSaved) {
    controller.onSaved(result)
  }

  return result
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
      executeItemFunction(gqltype, collectionField, objectId, session, materializedModel.collectionFields[collectionField].deleted, operations.DELETE)
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
        onSaveObject(typesDict.types[collectionGQLType.name].model, collectionGQLType, typesDict.types[collectionGQLType.name].controller, collectionItem, session, (item) => {
          item[connectionField] = objectId
        })
      }
      break
    case operations.UPDATE:
      operationFunction = collectionItem => {
        onUpdateSubject(typesDict.types[collectionGQLType.name].model, collectionGQLType, typesDict.types[collectionGQLType.name].controller, collectionItem, session, (item) => {
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
          return executeOperation(type.model, type.gqltype, type.controller, args.input, operations.SAVE)
        }
      }
      rootQueryArgs.fields['delete' + type.simpleEntityEndpointName] = {
        type: type.gqltype,
        args: { id: { type: new GraphQLNonNull(GraphQLID) } },
        async resolve (parent, args) {
          return executeOperation(type.model, type.gqltype, type.controller, args.id, operations.DELETE)
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
          return executeOperation(type.model, type.gqltype, type.controller, args.input, operations.UPDATE)
        }
      }
    }
  }

  return new GraphQLObjectType(rootQueryArgs)
}

const generateSchemaDefinition = function (gqlType) {
  const argTypes = gqlType.getFields()

  const schemaArg = {}

  for (const fieldEntryName in argTypes) {
    const fieldEntry = argTypes[fieldEntryName]

    if (fieldEntry.type === GraphQLID || isNonNullOfTypeForNotScalar(fieldEntry.type, GraphQLID)) {
      schemaArg[fieldEntryName] = mongoose.Schema.Types.ObjectId
    } else if (fieldEntry.type === GraphQLString || isNonNullOfTypeForNotScalar(fieldEntry.type, GraphQLString)) {
      schemaArg[fieldEntryName] = String
    } else if (fieldEntry.type === GraphQLInt || isNonNullOfTypeForNotScalar(fieldEntry.type, GraphQLInt)) {
      schemaArg[fieldEntryName] = Number
    } else if (fieldEntry.type === GraphQLFloat || isNonNullOfTypeForNotScalar(fieldEntry.type, GraphQLFloat)) {
      schemaArg[fieldEntryName] = Number
    } else if (fieldEntry.type === GraphQLBoolean || isNonNullOfTypeForNotScalar(fieldEntry.type, GraphQLBoolean)) {
      schemaArg[fieldEntryName] = Boolean
    } else if (fieldEntry.type instanceof GraphQLObjectType || isNonNullOfType(fieldEntry.type, GraphQLObjectType)) {
      if (fieldEntry.extensions && fieldEntry.extensions.relation) {
        if (!fieldEntry.extensions.relation.embedded) {
          schemaArg[fieldEntry.extensions.relation.connectionField] = mongoose.Schema.Types.ObjectId
        } else {
          let entryType = fieldEntry.type
          if (entryType instanceof GraphQLNonNull) {
            entryType = entryType.ofType
          }
          if (entryType !== gqlType) {
            schemaArg[fieldEntryName] = generateSchemaDefinition(entryType)
          } else {
            throw new Error('A type cannot have a field of its same type and embedded')
          }
        }
      }
    } else if (fieldEntry.type instanceof GraphQLList) {
      if (fieldEntry.extensions && fieldEntry.extensions.relation) {
        if (fieldEntry.extensions.relation.embedded) {
          const entryType = fieldEntry.type.ofType
          if (entryType !== gqlType) {
            schemaArg[fieldEntryName] = [generateSchemaDefinition(entryType)]
          } else {
            throw new Error('A type cannot have a field of its same type and embedded')
          }
        }
      }
    }
  }

  return schemaArg
}

const generateModel = function (gqlType, onModelCreated) {
  const model = mongoose.model(gqlType.name, generateSchemaDefinition(gqlType), gqlType.name)
  if (onModelCreated) {
    onModelCreated(model)
  }
  model.createCollection()
  return model
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

module.exports.getModel = function (gqltype) {
  return typesDict.types[gqltype.name].model
}

module.exports.connect = function (model, gqltype, simpleEntityEndpointName, listEntitiesEndpointName, controller, onModelCreated) {
  waitingInputType[gqltype.name] = {
    model: model,
    gqltype: gqltype
  }
  typesDict.types[gqltype.name] = {
    model: model || generateModel(gqltype, onModelCreated),
    gqltype: gqltype,
    simpleEntityEndpointName: simpleEntityEndpointName,
    listEntitiesEndpointName: listEntitiesEndpointName,
    endpoint: true,
    controller: controller
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
const buildMatchesClause = function (fieldname, operator, value) {
  const matches = {}

  if (operator === QLOperator.getValue('EQ').value || !operator) {
    matches[fieldname] = value
  } else if (operator === QLOperator.getValue('LT').value) {
    matches[fieldname] = { $lt: value }
  } else if (operator === QLOperator.getValue('GT').value) {
    matches[fieldname] = { $gt: value }
  } else if (operator === QLOperator.getValue('LTE').value) {
    matches[fieldname] = { $lte: value }
  } else if (operator === QLOperator.getValue('GTE').value) {
    matches[fieldname] = { $gte: value }
  } else if (operator === QLOperator.getValue('NE').value) {
    matches[fieldname] = { $ne: value }
  } else if (operator === QLOperator.getValue('BTW').value) {
    matches[fieldname] = { $gte: value[0], $lte: value[1] }
  } else if (operator === QLOperator.getValue('IN').value) {
    matches[fieldname] = { $in: value }
  } else if (operator === QLOperator.getValue('NIN').value) {
    matches[fieldname] = { $nin: value }
  } else if (operator === QLOperator.getValue('LIKE').value) {
    matches[fieldname] = { $regex: '.*' + value + '.*' }
  }

  return matches
}

const buildQueryTerms = async function (filterField, qlField, fieldName) {
  const aggregateClauses = {}
  const matchesClauses = {}

  if (qlField.type instanceof GraphQLScalarType || isNonNullOfType(qlField.type, GraphQLScalarType)) {
    matchesClauses[fieldName] = buildMatchesClause(fieldName, filterField.operator, filterField.value)
  } else if (qlField.type instanceof GraphQLObjectType || qlField.type instanceof GraphQLList || isNonNullOfType(qlField.type, GraphQLObjectType)) {
    let fieldType = qlField.type

    if (fieldType instanceof GraphQLList || fieldType instanceof GraphQLNonNull) {
      fieldType = qlField.type.ofType
    }

    filterField.terms.forEach(term => {
      if (qlField.extensions && qlField.extensions.relation && !qlField.extensions.relation.embedded) {
        const model = typesDict.types[fieldType.name].model
        const collectionName = model.collection.collectionName
        const localFieldName = qlField.extensions.relation.connectionField
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
        matchesClauses[fieldName] = buildMatchesClause(fieldName + '.' + term.path, term.operator, term.value)
      } else {
        let currentGQLPathFieldType = qlField.type
        if (currentGQLPathFieldType instanceof GraphQLList) {
          currentGQLPathFieldType = currentGQLPathFieldType.ofType
        }
        let aliasPath = fieldName
        let embeddedPath = ''

        term.path.split('.').forEach((pathFieldName) => {
          const pathField = currentGQLPathFieldType.getFields()[pathFieldName]
          if (pathField.type instanceof GraphQLScalarType || isNonNullOfType(pathField.type, GraphQLScalarType)) {
            matchesClauses[aliasPath + '_' + pathFieldName] = buildMatchesClause(aliasPath + (embeddedPath !== '' ? '.' + embeddedPath + '.' : '.') + pathFieldName, term.operator, term.value)
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

              if (!aggregateClauses[aliasPath]) {
                let lookup = {}
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
          }
        })
      }
    })
  }
  return { aggregateClauses: aggregateClauses, matchesClauses: matchesClauses }
}
