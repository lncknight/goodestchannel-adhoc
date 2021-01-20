let { chain, get, isNumber, size } = require('lodash')
let bb = require('bluebird')
let mysql = require('mysql')
let moment = require('moment')
let MongoClient = require('mongodb').MongoClient
let fs = require('fs')
let dotenv = require('dotenv')

dotenv.config()

const {
  MONGO_HOST,
  MONGO_USER,
  MONGO_PASS,
  MONGO_DBNAME,
  MONGO_PORT,
} = process.env

let connectDbs = async () => {
  try {
    let mongoUrl = `mongodb://${MONGO_HOST}:${MONGO_PORT}/${MONGO_DBNAME}`
    let mongoClient = await MongoClient.connect(mongoUrl, {
      useUnifiedTopology: true
    })

    mongoDb = mongoClient.db(MONGO_DBNAME)

    return {
      mongoDb,
    }
  }
  catch (err) {
    console.log(err)
  }
}

let getVideoListFromTarget = async ({
  mongoDb
}) => {

  let rs = await mongoDb.collection('article').find({
    'extra.anvatoId': {
      $exists: true
    }
  }, {
    extra: 1
  })
    // .limit(100)
    .toArray()

  return rs
}

let getVideoMetaFromSource = async ({
  article
}) => {

  // e.g. XXXX_34444444
  // get last part ?
  let anvatoIdHaystack = get(article, 'extra.anvatoId')
  let tmp = chain(anvatoIdHaystack)
    .split('_')
    .last()
    .trim()
    .parseInt()
    .value()
  let anvatoId  
  if (isNumber(tmp)) {
    anvatoId = tmp
  }

  // TODO mysql
  let videoUrl = `TODO url from MYSQL by #${get(article, 'extra.anvatoId')}`

  return {
    videoUrl,
    anvatoId,
    anvatoIdHaystack,
  }
}

let saveTarget = async ({
  dryRun = true,
  anvatoId,
  videoUrl,
}) => {
  // rename extra.anvatoId -> extra.anvatoIdOrig
  // videoUrl -> extra.anvatoId

  log(`${dryRun ? '[DRYRUN] ' : ''}saving to target db, anvatoId: ${anvatoId}, videoUrl: ${videoUrl}`)

  // TODO save backup

  let ObjectId = 'object id/...'
  return {
    ObjectId
  }
}

let log = async (message) => {
  message = `${moment().format('Y-MM-DD HH:mm:ss')} - ${message}\n`
  fs.appendFileSync('./logs/default.log', message)
}

let main = async () => {
  log(`============== starting ${new Date()} ===============`)

  let { mongoDb } = await connectDbs()

  let articles = await getVideoListFromTarget({
    mongoDb
  })

  log(`found ${size(articles)} articles, converting..`)

  await bb.map(articles, async article => {
    let articleId = get(article, '_id')
    try {
      let { videoUrl, anvatoId, anvatoIdHaystack } = await getVideoMetaFromSource({ article })

      if (!anvatoId) {
        throw new Error(`anvatoId is null or cannot be parsed, anvatoIdHaystack: ${anvatoIdHaystack}`)
      }

      if (!videoUrl) {
        throw new Error('video url not found from mapping db')
      }

      await saveTarget({
        dryRun: true,
        anvatoId,
        videoUrl,
      })
    }
    catch (err) {
      log(`[ERROR] @ #${articleId}, ${err.message}`)
    }
  }, {
    concurrency: 10,
  })

  console.log('DONE')
}

main()