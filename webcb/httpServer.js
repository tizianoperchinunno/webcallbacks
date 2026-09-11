const http = require('http');

// TLS terminato a monte su Akamai + ALB: questo listener in chiaro opera
// solo sull'hop interno alla VPC, raggiungibile esclusivamente dal security
// group dell'ALB (vedi .snyk, sezione [7], per il dettaglio completo del
// rischio accettato). Isolato in un file proprio perche' l'esclusione di
// Snyk Code (.snyk -> exclude) lavora solo a livello di intero file, non di
// singola riga: un commento inline (`deepcode ignore`) non viene onorato
// dalle versioni correnti di Snyk Code.
function createHttpServer(app) {
  return http.createServer(app);
}

module.exports = createHttpServer;
