# How to
Install
```bash
npm install @simtlix/gnx --save
```

To use this lib:
* Define your mongoose models
* Define your GraphQL types
* Register models and types using `connect` function for *non embedded* types and `addNoEndpointType` function for *embedded* ones
* Create the GraphQL schema using `createSchema` function

# Example
There is a sample of an app using this lib at [gnx-sample](https://github.com/simtlix/gnx-sample)
