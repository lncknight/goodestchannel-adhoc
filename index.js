let { chain, get, isNumber, size } = require('lodash')
let bb = require('bluebird')
let mysql = require('mysql2/promise')
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

  MYSQL_HOST,
  MYSQL_USER,
  MYSQL_PASS,
  MYSQL_DBNAME,
  MYSQL_PORT,
} = process.env

let connectDbs = async () => {
  try {
    let mongoUrl = `mongodb://${MONGO_HOST}:${MONGO_PORT}/${MONGO_DBNAME}`
    let mongoClient = await MongoClient.connect(mongoUrl, {
      useUnifiedTopology: true
    })
    mongoDb = mongoClient.db(MONGO_DBNAME)


    // create the connection, specify bluebird as Promise
    const mysqlPool = await mysql.createPool({
      host: MYSQL_HOST,
      user: MYSQL_USER,
      password: MYSQL_PASS,
      port: MYSQL_PORT,
      database: MYSQL_DBNAME,
      Promise: bb,
      connectionLimit: 10,
    });

    return {
      mongoDb,
      mysqlPool,
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
  article,
  mysqlPool,
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

  // mysql
  // let videoUrl = `TODO url from MYSQL by #${get(article, 'extra.anvatoId')}`
  // const [rows, fields] = await mysqlPool.query('SELECT * FROM `wp_lvb_posts` WHERE `id` = ?', ['2']);
  const [rows, fields] = await mysqlPool.query(`select * from anvato2jwplayer where anvato_id = ?`, [`MCP1_${anvatoId}`]);
  let videos = get(rows, '0.mediaFiles')
  // let jsonStr = get(rows, '0.mediaFiles')
  // let videos = JSON.parse(jsonStr)
  let videoUrl = chain(videos)
    .map(video => {
      return get(video, 'data.url')
    })
    .filter()
    .first()
    .value()

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

const logFile = `./logs/default-${moment().format('YMMDD_HHmmss')}.log`
let log = async (message) => {
  message = `${moment().format('Y-MM-DD HH:mm:ss')} - ${message}\n`
  fs.appendFileSync(logFile, message)
}

let main = async () => {
  let tStart = new Date()

  log(`============== starting ${new Date()} ===============`)

  let { mongoDb, mysqlPool } = await connectDbs()

  let articles = await getVideoListFromTarget({
    mongoDb
  })

  log(`found ${size(articles)} articles, converting..`)

  await bb.map(articles, async article => {
    let articleId = get(article, '_id')
    try {
      let { videoUrl, anvatoId, anvatoIdHaystack } = await getVideoMetaFromSource({ mysqlPool, article })

      if (!anvatoId) {
        throw new Error(`anvatoId is null or cannot be parsed, anvatoIdHaystack: ${anvatoIdHaystack}`)
      }

      if (!videoUrl) {
        throw new Error(`video url not found from mapping db, anvatoId: ${anvatoId}`)
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

  log(`============== DONE used ${new Date().getTime() - tStart}ms ===============`)
}

main()