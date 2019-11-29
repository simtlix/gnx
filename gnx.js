const graphql = require('graphql')
const mongoose = require('mongoose')

const {
  GraphQLObjectType, GraphQLString, GraphQLID, GraphQLSchema, GraphQLList,
  GraphQLNonNull, GraphQLInputObjectType, GraphQLScalarType, Kind
} = graphql

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
  parseLiteral(ast) {
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

function parseQLValue(value) {
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

const buildInputType = function (model, gqltype) {
  const argTypes = gqltype.getFields()

  const fieldsArgs = {}

  for (const fieldEntryName in argTypes) {
    const fieldEntry = argTypes[fieldEntryName]
    const fieldArg = {}

    if (fieldEntry.type instanceof GraphQLScalarType || fieldEntry.type instanceof GraphQLNonNull) {
      fieldArg.type = fieldEntry.type
    } else if (fieldEntry.type instanceof GraphQLObjectType) {
      if (fieldEntry.extensions && fieldEntry.extensions.relation) {
        if (!fieldEntry.extensions.relation.embedded) {
          fieldArg.type = IdInputType
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
      resolve(parent, args) {
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

      if (fieldEntry.type instanceof GraphQLScalarType || fieldEntry.type instanceof GraphQLNonNull) {
        argsObject[fieldEntryName].type = QLFilter
      } else if (fieldEntry.type instanceof GraphQLObjectType || fieldEntry.type instanceof GraphQLList) {
        argsObject[fieldEntryName].type = QLTypeFilterExpression
      }
    }

    rootQueryArgs.fields[type.listEntitiesEndpointName] = {
      type: new GraphQLList(type.gqltype),
      args: argsObject,
      async resolve(parent, args) {

        let result = await buildQuery(args, type.gqltype)
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

    if (fieldEntry.type instanceof GraphQLScalarType || fieldEntry.type instanceof GraphQLNonNull) {
      modelArgs[fieldEntryName] = args[fieldEntryName]
    } else if (fieldEntry.type instanceof GraphQLObjectType) {
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

const saveObject = async function (Model, gqltype, args) {
  const session = await mongoose.startSession()
  session.startTransaction()
  try {
    const newObject = await onSaveObject(Model, gqltype, args, session)
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

const onSaveObject = async function (Model, gqltype, args, session, linkToParent) {
  const result = materializeModel(args, gqltype, linkToParent)
  const newObject = new Model(result.modelArgs)
  console.log(JSON.stringify(newObject))
  newObject.$session(session)

  if (result.collectionFields) {
    for (const collectionField in result.collectionFields) {
      if (result.collectionFields[collectionField].added) {
        const argTypes = gqltype.getFields()
        const collectionGQLType = argTypes[collectionField].type.ofType
        const connectionField = argTypes[collectionField].extensions.relation.connectionField

        result.collectionFields[collectionField].added.forEach(collectionItem => {
          onSaveObject(typesDict.types[collectionGQLType.name].model, collectionGQLType, collectionItem, session, (item) => {
            item[connectionField] = newObject._id
          })
        })
      }
      if (result.collectionFields[collectionField].updated) {
        const argTypes = gqltype.getFields()
        const collectionGQLType = argTypes[collectionField].type.ofType
        const connectionField = argTypes[collectionField].extensions.relation.connectionField

        result.collectionFields[collectionField].updated.forEach(collectionItem => {
          onSaveObject(typesDict.types[collectionGQLType.name].model, collectionGQLType, collectionItem, session, (item) => {
            item[connectionField] = newObject._id
          })
        })
      }
      if (result.collectionFields[collectionField].deleted) {
        result.collectionFields[collectionField].deleted.forEach(collectionItem => {
          // TODO
        })
      }
    }
  }
  return newObject.save()
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
        async resolve(parent, args) {
          return saveObject(type.model, type.gqltype, args.input)
        }
      }
      rootQueryArgs.fields['update' + type.simpleEntityEndpointName] = {
        type: type.gqltype,
        args: argsObject,
        async resolve(parent, args) {
          return saveObject(type.model, type.gqltype, args.input)
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

const buildQuery = async function (input, gqltype) {
  const aggreagteClauses = []
  const matchesClauses = []


  for (const key in input) {
    if (input.hasOwnProperty(key)) {
      const filterField = input[key]
      const qlField = gqltype.getFields()[key]

      let result = await buildQueryTerms(filterField, qlField, key)

      if(result)
      {
        for(aggregate in result.aggregateClauses){
          aggreagteClauses.push(result.aggregateClauses[aggregate].lookup)
          aggreagteClauses.push(result.aggregateClauses[aggregate].unwind)
        }

        for (const match in result.matchesClauses) {
          if (result.matchesClauses.hasOwnProperty(match)) {
            const matchClause = result.matchesClauses[match]
            matchesClauses.push(matchClause)
          }
        }
      }

      console.log(JSON.stringify(result))

    }
  }
}

const buildQueryTerms = async function (filterField, qlField, fieldName) {

  const aggregateClauses = {}
  const matchesClauses = {}

  if (qlField.type instanceof GraphQLScalarType) {
    let matchesClause = {}
    //TODO only equal for now
    matchesClause[fieldName] = filterField.value
    matchesClauses[fieldName] = matchesClause

  } else if (qlField.type instanceof GraphQLObjectType || qlField.type instanceof GraphQLList) {

        let fieldType = qlField.type
        
        if(fieldType instanceof GraphQLList){
          fieldType = qlField.type.ofType
        }

        filterField.terms.forEach(term => {
        let model = typesDict.types[fieldType.name].model
        let collectionName = model.collection.collectionName
        let localFieldName = qlField.extensions.relation.connectionField;

        if (qlField.extensions && qlField.extensions.relation && !qlField.extensions.relation.embedded) {
          if (!aggregateClauses[fieldName]) {
            let lookup = {}

            if(qlField.type instanceof GraphQLList){
              lookup = {
                $lookup: {
                  from: collectionName,
                  foreignField: localFieldName,
                  localField: '_id',
                  as: fieldName
                }
              }
            }else{
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
              'lookup': lookup,
              'unwind': { $unwind: { path: "$" + fieldName, preserveNullAndEmptyArrays: true } }
            }

          }
        }
        //autor:{terms{path:city.name}}

        if (term.path.indexOf(".") < 0) {
          let matchesClause = {}
          matchesClause[fieldName + "." + term.path] = term.value
          matchesClauses[fieldName] = matchesClause
        } else {
          let currentGQLPathFieldType = qlField.type;
          let aliasPath = fieldName
          let embeddedPath = "";

          term.path.split(".").forEach((pathFieldName) => {
            let pathField = currentGQLPathFieldType.getFields()[pathFieldName]
            if (pathField.type instanceof GraphQLScalarType) {
              let matchesClause = {}
              matchesClause[aliasPath + (embeddedPath!=""? "." + embeddedPath + "." : ".")  + pathFieldName] = term.value
              matchesClauses[aliasPath + "_" + pathFieldName] = matchesClause
            } else if (pathField.type instanceof GraphQLObjectType || pathField.type instanceof GraphQLList) {
              let pathFieldType = pathField.type
              if (pathField.type instanceof GraphQLList) {
                pathFieldType = pathField.type.ofType
              }
              if (pathField.extensions && pathField.extensions.relation && !pathField.extensions.relation.embedded) {
                let currentPath = aliasPath
                aliasPath += "_" + pathFieldName
                let pathModel = typesDict.types[pathFieldType.name].model
                let fieldPathCollectionName = pathModel.collection.collectionName
                let pathLocalFieldName = pathField.extensions.relation.connectionField

                if (!aggregateClauses[aliasPath]) {
                  let lookup = {}
                  if(qlField.type instanceof GraphQLList){
                    lookup = {
                      $lookup: {
                        from: fieldPathCollectionName,
                        foreignField: currentPath + "." + pathLocalFieldName,
                        localField: '_id',
                        as: aliasPath
                      }
                    }
                  }else{
                    lookup = {
                      $lookup: {
                        from: fieldPathCollectionName,
                        foreignField: '_id',
                        localField: currentPath + "." + pathLocalFieldName,
                        as: aliasPath
                      }
                    }
                  }

                  aggregateClauses[aliasPath] = {
                    'lookup': lookup,
                    'unwind': { $unwind: { path: "$" + aliasPath, preserveNullAndEmptyArrays: true } }
                  }

                }

              } else {
                if (embeddedPath == "") {
                  embeddedPath += pathFieldName
                }
                else {
                  embeddedPath += "." + pathFieldName
                }
              }
            } 


          }

          )
        }
      

    })






  }


  return { 'aggregateClauses': aggregateClauses, 'matchesClauses': matchesClauses }

}