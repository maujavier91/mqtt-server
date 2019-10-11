const express = require('express');
const mysql= require('mysql');
const http = require('http');
const socket_io = require('socket.io'); 
const mqtt = require('mqtt');
const moment = require('moment');

const app = express();
const server = http.createServer(app);
const io = socket_io(server);

const faker = require('faker');



var connection = mysql.createConnection({
    host     : 'localhost',
    user     : 'root',
    password : '',
    database : 'testdbmqtt'
  });
  

const client = mqtt.connect('mqtt://test.mosquitto.org');
client.on('connect', ()=> {
  client.subscribe('MPT1/out', function (err) {
    if (!err) {
      console.log('success')
    } else console.log('error in establishing connection')
  })})

  const datafields = ['Datetime',
                      'Range 1',
                      'Range 2', 
                      'gx', 
                      'gy',
                      'gz',
                      'ax',
                      'ay',
                      'az',
                      'Pitch',
                      'Roll',
                      'Temp',
                      'VBat',
                      'RSSI'];

  let now = moment()
setInterval(() => {
  let testData={
    Datetime: now.format('YYYY-MM-DD HH:mm:ss'),
  'Range 1': faker.finance.amount(0, 30, 3),
  'Range 2': faker.finance.amount(0, 30, 3),
  gx: faker.random.number(50),
  gy: faker.random.number(50),
  gz: faker.random.number(50),
  ax: faker.random.number(50),
  ay: faker.random.number(50),
  az: faker.random.number(50),
  Pitch: faker.finance.amount(0, 30, 3),
  Roll: faker.finance.amount(0, 30, 3),
  Temp: faker.finance.amount(0, 30, 3),
  VBat: faker.finance.amount(0, 30, 3),
  RSSI: faker.finance.amount(0, 30, 3)
}
  io.emit('actualizacion', testData)
  now.add(3, 'minutes')
}, 3000);
  client.on('message', (topic, message) =>{
    let msgArray = message.toString().split(',');
    let formattedDatetime = moment(msgArray[0] + msgArray[1], 'DDMMYYYYHHmmss').format('YYYY-MM-DD HH:mm:ss')
    msgArray.splice(0, 2, formattedDatetime);
    let dataUnit={};
    datafields.forEach((key, index) => {
      dataUnit[key]=msgArray[index];
    })
      connection.query('INSERT INTO mqttdata VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',msgArray,
      (error, results, fields)=>{
          if(error){
              console.log(error);
          }
          
      });
      io.emit('actualizacion', dataUnit);
      
  }) 

  /* GET users listing. */
/* app.get('/data', (req, res, next) => 
 {
  connection.query('SELECT * FROM mqttdata', (err,results,fields) => {
    if (err) throw err;
    res.send(results);
  })
}); */



  server.listen(3001, () => {
    console.log('Listening on port 3001');
  });