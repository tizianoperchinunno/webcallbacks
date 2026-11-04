const winston = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');

// Tutti i log vanno solo nei file giornalieri di logs/ (ruotati, compressi e tenuti
// 20 giorni). Prima ogni riga veniva copiata anche su stdout: sotto pm2 finiva in
// ~/.pm2/logs/*-out.log, che non viene mai ruotato ed era arrivato a oltre 100 GB.
// Per vedere i log anche a terminale (es. in sviluppo) avviare con LOG_CONSOLE=true.
const transports = [
  new DailyRotateFile({
    filename: 'logs/application-%DATE%.log',
    datePattern: 'YYYY-MM-DD',
    zippedArchive: true,
    maxSize: '20m',
    maxFiles: '20d',
    // anche gli errori non gestiti (crash) finiscono nel file invece che su stderr;
    // dopo averli scritti winston termina il processo, come faceva Node
    handleExceptions: true,
    handleRejections: true,
  }),
];
if (process.env.LOG_CONSOLE === 'true') {
  transports.push(new winston.transports.Console({ handleExceptions: true, handleRejections: true }));
}

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `[${timestamp}] ${level.toUpperCase()}: ${message}`;
    })
  ),
  transports,
});

module.exports = logger;
