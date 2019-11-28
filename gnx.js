const graphql = require('graphql')
const mongoose = require('mongoose')

const {
  GraphQLObjectType, GraphQLString, GraphQLID, GraphQLSchema, GraphQLList,
  GraphQLNonNull, GraphQLInputObjectType, GraphQLScalarType, Kind
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
      return parseBoolean(ast.value)
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

  for (const fieldEntryName in argTypes) {
    const fieldEntry = argTypes[fieldEntryName]
    const fieldArg = {}

    if (fieldEntry.type instanceof GraphQLScalarType || isNonNullOfType(fieldEntry.type, GraphQLScalarType)) {
      fieldArg.type = fieldEntry.type
    } else if (fieldEntry.type instanceof GraphQLObjectType || isNonNullOfType(fieldEntry.type, GraphQLObjectType)) {
      if (fieldEntry.extensions && fieldEntry.extensions.relation) {
        if (!fieldEntry.extensions.relation.embedded) {
          fieldArg.type = fieldEntry.type instanceof GraphQLNonNull ? new GraphQLNonNull(IdInputType) : IdInputType
        } else if (typesDict.types[fieldEntry.type.name].inputType) {
          fieldArg.type = typesDict.types[fieldEntry.type.name].inputType
        } else {
          return null
        }
      } else {
        console.warn('Configuration issue: Field ' + fieldEntryName + ' does not define extensions.relation')
      }
    } else if (fieldEntry.type instanceof GraphQLList) {
      const ofType = fieldEntry.type.ofType

      if (typesDict.types[ofType.name].inputType) {
        if (!fieldEntry.extensions || !fieldEntry.extensions.relation || !fieldEntry.extensions.relation.embedded) {
          const oneToMany = new GraphQLInputObjectType({
            name: 'OneToMany' + fieldEntryName,
            fields: () => ({
              added: { type: new GraphQLList(typesDict.types[ofType.name].inputType) },
              updated: { type: new GraphQLList(typesDict.types[ofType.name].inputType) },
              deleted: { type: new GraphQLList(typesDict.types[ofType.name].inputType) }
            })
          })

          fieldArg.type = oneToMany
        } else if (fieldEntry.extensions && fieldEntry.extensions.relation && fieldEntry.extensions.relation.embedded) {
          fieldArg.type = new GraphQLList(typesDict.types[ofType.name].inputType)
        }
      } else {
        return null
      }
    }

    if (fieldArg.type) {
      fieldsArgs[fieldEntryName] = fieldArg
    }
  }

  const inputTypeBody = {
    name: gqltype.name + 'Input',
    fields: fieldsArgs
  }

  return new GraphQLInputObjectType(inputTypeBody)
}

const buildPendingInputTypes = function (waitingInputType) {
  const stillWaitingInputType = {}
  let isThereAtLeastOneWaiting = false

  for (const pendingInputTypeName in waitingInputType) {
    const model = waitingInputType[pendingInputTypeName].model
    const gqltype = waitingInputType[pendingInputTypeName].gqltype
    const builtInputType = buildInputType(model, gqltype)

    if (builtInputType) {
      typesDict.types[gqltype.name].inputType = builtInputType
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

    rootQueryArgs.fields[type.listEntitiesEndpointName] = {
      type: new GraphQLList(type.gqltype),
      args: argsObject,
      resolve (parent, args) {
        return type.model.find({})
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

      if (fieldEntry.extensions && fieldEntry.extensions.relation) {
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
  session.startTransaction()
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
      // TODO: implement
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

const onUpdateSubject = async function (Model, gqltype, args, session, linkToParent) {
  const materializedModel = materializeModel(args, gqltype, linkToParent)
  const objectId = args.id

  if (materializedModel.collectionFields) {
    iterateonCollectionFields(materializeModel, gqltype, objectId, session)
  }

  const modifiedObject = materializedModel.modelArgs
  const currentObject = await Model.findById({ _id: objectId })

  const argTypes = gqltype.getFields()
  for (const fieldEntryName in argTypes) {
    const fieldEntry = argTypes[fieldEntryName]
    if (fieldEntry.extensions && fieldEntry.extensions.relation) {
      if (fieldEntry.extensions.relation.embedded) {
        const oldObjectData = currentObject[fieldEntryName]
        const newObjectData = modifiedObject[fieldEntryName]
        if (Array.isArray(oldObjectData) && Array.isArray(newObjectData)) {
          modifiedObject[fieldEntryName] = newObjectData
        } else {
          modifiedObject[fieldEntryName] = { ...oldObjectData, ...newObjectData }
        }
      }
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
    iterateonCollectionFields(materializeModel, gqltype, newObject._id, session)
  }

  return newObject.save()
}

const iterateonCollectionFields = function (materializedModel, gqltype, objectId, session) {
  for (const collectionField in materializedModel.collectionFields) {
    if (materializedModel.collectionFields[collectionField].added) {
      const argTypes = gqltype.getFields()
      const collectionGQLType = argTypes[collectionField].type.ofType
      const connectionField = argTypes[collectionField].extensions.relation.connectionField

      materializedModel.collectionFields[collectionField].added.forEach(collectionItem => {
        onSaveObject(typesDict.types[collectionGQLType.name].model, collectionGQLType, collectionItem, session, (item) => {
          item[connectionField] = objectId
        })
      })
    }
    if (materializedModel.collectionFields[collectionField].updated) {
      const argTypes = gqltype.getFields()
      const collectionGQLType = argTypes[collectionField].type.ofType
      const connectionField = argTypes[collectionField].extensions.relation.connectionField

      materializedModel.collectionFields[collectionField].updated.forEach(collectionItem => {
        onUpdateSubject(typesDict.types[collectionGQLType.name].model, collectionGQLType, collectionItem, session, (item) => {
          item[connectionField] = objectId
        })
      })
    }
    if (materializedModel.collectionFields[collectionField].deleted) {
      materializedModel.collectionFields[collectionField].deleted.forEach(collectionItem => {
        // TODO
      })
    }
  }
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
}

module.exports.addNoEndpointType = function (gqltype) {
  waitingInputType[gqltype.name] = {
    gqltype: gqltype
  }

  typesDict.types[gqltype.name] = {
    gqltype: gqltype,
    endpoint: false
  }
}
