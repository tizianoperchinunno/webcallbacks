require('dotenv').config();
// TLS terminato a monte (load balancer): il worker ascolta in chiaro sulla rete privata.
const createHttpServer = require('./httpServer');
const fs = require('fs');
const express = require('express');
const cluster = require('cluster');
const os = require('os');
const path = require('path');
const url = require('url');

const numCPUs = os.cpus().length;

const Redis = require('ioredis');

// Variabili obbligatorie: se mancano, meglio fermarsi subito
const REQUIRED_ENV_VARS = ['REDIS_HOST', 'REDIS_PORT', 'GENESYS_CLIENT_ID', 'GENESYS_CLIENT_SECRET'];
const missingEnvVars = REQUIRED_ENV_VARS.filter((name) => !process.env[name]);
if (missingEnvVars.length > 0) {
    throw new Error('Variabili ambiente mancanti: ' + missingEnvVars.join(', '));
}

// clientId per Basic Auth verso l'endpoint OAuth di Genesys Cloud,
// costruito a runtime da client id/secret separati (mai committare il valore gi� codificato)
const clientId = Buffer.from(
    process.env.GENESYS_CLIENT_ID + ':' + process.env.GENESYS_CLIENT_SECRET
).toString('base64');

// fetch e' globale da Node 18: nessuna dipendenza esterna necessaria
const logger = require('./logger');

// Configura la connessione al tuo cluster ElastiCache
const redis = new Redis({
    host: process.env.REDIS_HOST,
    port: Number(process.env.REDIS_PORT),
    password: process.env.REDIS_PASSWORD || undefined,
    retryStrategy: (times) => {
        return Math.min(times * 100, 1000); // Ritenta ogni 100 ms fino a 2 secondi
    },	
});

redis.on('connect', () => {
  logger.info('Connesso a Redis');
});
redis.on('error', (err) => {
	logger.info('Errore di connessione a Redis: ' + JSON.stringify(err));
});	
redis.on('reconnecting', () => {
  logger.info('Tentativo di riconnessione a Redis');
});

if (cluster.isMaster) {
  logger.info(`Master ${process.pid} is running`);

  // Creazione dei worker
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    logger.info(`Worker ${worker.process.pid} died`);
    // Se un worker termina, ne viene avviato uno nuovo per sostituirlo
    cluster.fork();
  });
} else {
  // Codice del worker
  const app = express();
  // non esporre il framework in uso agli attaccanti
  app.disable('x-powered-by');

  app.use(express.static(path.join(__dirname, 'public')));
  app.use(express.json({ limit: '10kb' }));
  

  app.get('/healthcheck', async (req, res) => {
	await writeToCache("status","ok");
	let status=await readFromCache("status");  
    res.json({ 'status': status });
  })  
      
const allowedOrigins = ['https://webcb-stage.costacrociere.it','https://webcb.costacrociere.it','https://www.costacruises.fi','https://www.costacruzeiros.com.pt','https://www.costacroisieres.be','https://www.costacruises.co.uk',
'https://www.costacruceros.com','https://www.costacruises.ru','https://www.costacruzeiros.com','https://www.costacruises.nl','https://www.costakreuzfahrten.ch','https://www.costakreuzfahrten.at',
'https://www.costacruises.com','https://www.costakreuzfahrten.de','https://www.costacroisieres.fr','https://www.costacruceros.es','https://www.costacruises.eu','https://www.costacrociere.it',
'https://aem-stage2.costacruises.fi','https://aem-stage2.costacruzeiros.com.pt','https://aem-stage2.costacroisieres.be','https://aem-stage2.costacruises.co.uk','https://aem-stage2.costacruceros.com',
'https://aem-stage2.costacruises.ru','https://aem-stage2.costacruzeiros.com','https://aem-stage2.costacruises.nl','https://aem-stage2.costakreuzfahrten.ch','https://aem-stage2.costakreuzfahrten.at',
'https://aem-stage2.costacruises.com','https://aem-stage2.costakreuzfahrten.de','https://aem-stage2.costacroisieres.fr','https://aem-stage2.costacruceros.es','https://aem-stage2.costacruises.eu',
'https://aem-stage2.costacrociere.it','https://aem-uat2.costacruises.fi','https://aem-uat2.costacruzeiros.com.pt','https://aem-uat2.costacroisieres.be','https://aem-uat2.costacruises.co.uk',
'https://aem-uat2.costacruceros.com','https://aem-uat2.costacruises.ru','https://aem-uat2.costacruzeiros.com','https://aem-uat2.costacruises.nl','https://aem-uat2.costakreuzfahrten.ch',
'https://aem-uat2.costakreuzfahrten.at','https://aem-uat2.costacruises.com','https://aem-uat2.costakreuzfahrten.de','https://aem-uat2.costacroisieres.fr','https://aem-uat2.costacruceros.es',
'https://aem-uat2.costacruises.eu','https://aem-uat2.costacrociere.it','https://dev-mycosta.costacrociere.it','https://dev-mycosta.costacroisieres.fr','https://dev-mycosta.costakreuzfahrten.de',
'https://qa-mycosta.costacrociere.it','https://qa-mycosta.costacroisieres.fr','https://qa-mycosta.costakreuzfahrten.de','https://stage-mycosta.costacrociere.it','https://stage-mycosta.costacroisieres.fr',
'https://stage-mycosta.costakreuzfahrten.de','https://mycosta.costacrociere.it','https://mycosta.costacroisieres.fr','https://mycosta.costakreuzfahrten.de','https://dev-mycosta.costacruceros.es',
'https://qa-mycosta.costacruceros.es','https://stage-mycosta.costacruceros.es','https://mycosta.costacruceros.es','https://mycosta-sales.costacruceros.es','https://mycosta-sales.costacroisieres.fr','https://mycosta-sales.costacrociere.it']
	
	// l'header Origin puo' mancare o essere ripetuto: normalizzarlo prima di usarlo
	const normalizeOrigin = (req) =>
		typeof req.headers.origin === 'string' ? req.headers.origin.toLowerCase() : '';

	app.options('*', (req, res) => {
		let origin = normalizeOrigin(req);
		if (allowedOrigins.includes(origin)) {
			res.setHeader('Access-Control-Allow-Origin', '*');
			res.setHeader('Access-Control-Allow-Methods', '*');
			res.setHeader('Access-Control-Allow-Headers', '*');			 
		}else{
			logger.info("-> KO|CORS " + origin);
		}  	  
	  res.send();
	});

  app.post('/ExternalLog', (req, res) => {
    const postData = req.body;
    if (postData.log) 
    	{
    		logger.info(postData.log);
    		res.json({ "status": "OK"});
    	}
    else
    	{res.json({ "status": "KO"});}
  });
  
  app.post('/echo', (req, res) => {
    const postData = req.body;
	console.log(req.body);
    res.json({ 'message': 'Received POST data', 'data': postData });
  });

  app.get('/echo', (req, res) => {
    res.json({ 'message': 'Received GET data'});
  });  
      
  const isFilledString = (v) => typeof v === 'string' && v.trim() !== '';
  const REQUIRED_FIELDS = ['Queue', 'Type', 'PhoneNumber', 'Country', 'Location'];
  const OPTIONAL_FIELDS = ['Subject', 'ScheduleDayTime', 'OptionId', 'FirstName',
                           'LastName', 'Email', 'ReasonWhy', 'ReasonDesc', 'PageType'];

  app.post('/callback', async (req, res) => {
  
  	const UUID=Math.round((new Date()).getTime());
	// variabili di richiesta: devono restare locali, altrimenti richieste
	// concorrenti nello stesso worker si sovrascrivono a vicenda
	let response, data;
	let origin = normalizeOrigin(req);
	if (allowedOrigins.includes(origin)) {
		res.setHeader('Access-Control-Allow-Origin', '*');
		res.setHeader('Access-Control-Allow-Methods', '*');
		res.setHeader('Access-Control-Allow-Headers', '*');			 
	}else{
		res.json({"status":"KO|CORS"});logger.info(UUID + "-> KO|CORS " + origin);
		return;
	}  
	 
    //const postData = req.body;
	logger.info(UUID + "-> " + JSON.stringify(req.body));
	// il client controlla il tipo dei campi, non solo il valore: senza questo
	// controllo un body come {"Country":{"length":5}} supera tutte le verifiche
	if (!REQUIRED_FIELDS.every((f) => isFilledString(req.body[f]))) {
		res.json({"status":"KO|missing params"});logger.info(UUID + "-> KO|missing params");
		return;
	}
	if (!OPTIONAL_FIELDS.every((f) => req.body[f] == null || typeof req.body[f] === 'string')) {
		res.json({"status":"KO|invalid params"});logger.info(UUID + "-> KO|invalid params");
		return;
	}
	if (req.body.ScheduleDayTime!=null && req.body.ScheduleDayTime!="" && !isIsoDate(req.body.ScheduleDayTime)){
		res.json({"status":"KO|ScheduleDayTime"});logger.info(UUID + "-> KO|ScheduleDayTime");
		return;
	}
	if (req.body.Type!="sch" && req.body.Type!="def" && req.body.Type!="opt" && req.body.Type!="now" && req.body.Type!="sms" && req.body.Type!="whatsapp"){
		res.json({"status":"KO|Type"});logger.info(UUID + "-> KO|Type");
		return;
	}
	if (typeof req.body.Country !== 'string' || req.body.Country.length==0 || req.body.Country.length>8){
		res.json({"status":"KO|Country"});logger.info(UUID + "-> KO|Country");
		return;
	}
	if (typeof req.body.PhoneNumber !== 'string' || req.body.PhoneNumber.length<7 || req.body.PhoneNumber.length>20){
		res.json({"status":"KO|PhoneNumber"});logger.info(UUID + "-> KO|PhoneNumber");
		return;
	}
	if (req.body.OptionId !=null && req.body.OptionId !="" &&  !(/^\d+$/.test(req.body.OptionId)) ){
		res.json({"status":"KO|OptionId"});logger.info(UUID + "-> KO|OptionId");
		return;
	}	
	var bearerToken=await readFromCache('token');
	
	let params={
	  "flowId": "c986a982-418d-469d-b2c8-ba4c06863378",
	  "inputData": {
		"Flow.PhoneNumber": req.body.PhoneNumber,
		"Flow.Country": req.body.Country,
		"Flow.Queue": req.body.Queue,
		"Flow.Subject": req.body.Subject,
		"Flow.Type": req.body.Type,
		"Flow.ScheduleDayTime": req.body.ScheduleDayTime,
		"Flow.OptionId": req.body.OptionId,
		"Flow.Location": req.body.Location,
		"Flow.FirstName": req.body.FirstName,
		"Flow.LastName": req.body.LastName,
		"Flow.Email": req.body.Email,
		"Flow.ReasonWhy": req.body.ReasonWhy,
		"Flow.ReasonDesc": req.body.ReasonDesc,
		"Flow.pageType": req.body.PageType
	  },
	  "name": "WebCallbackCreation"
	}
	
	
	if (bearerToken!="") 
	{
		response = await fetch('https://api.mypurecloud.ie/api/v2/flows/executions', {
			 method: 'POST',
				body: JSON.stringify(params),
				headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
		});		
		data = await response.json();
		logger.info(UUID + "-> " + JSON.stringify(data));
		if(response.status==200){
			res.json({"status":"OK|" + req.body.Type})
			return;
		}
	}	
	if (bearerToken=="" || response.status==401){
		logger.info(UUID + "-> " + "autenticazione non valida");
		response = await fetch('https://login.mypurecloud.ie/oauth/token', {
			 method: 'POST',
				body: 'grant_type=client_credentials',
				headers: { 'Content-Type': 'application/x-www-form-urlencoded','Authorization':'Basic ' + clientId }
		});

		data = await response.json();
		bearerToken=data.access_token;
		
		if (writeToCache('token', data.access_token)){
			logger.info(UUID + "-> " + "token recuperato da auth API");
			console.log("token recuperato da auth API")
		}else{
			logger.info(UUID + "-> " + "recupero token in errore");
			res.json({"status":"KO|na"})
		}	
	
		response = await fetch('https://api.mypurecloud.ie/api/v2/flows/executions', {
			 method: 'POST',
				body: JSON.stringify(params),
				headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
		});		
		data = await response.json();
		logger.info(UUID + "-> " + JSON.stringify(data));
		if(response.status==200){
			res.json({"status":"OK|" + req.body.Type});
			logger.info(UUID + "-> OK|" + req.body.Type);
		}else{
			res.json({"status":"KO|na"});
			logger.info(UUID + "-> KO|api execution");
		}
	}
   
  });
  
  

  app.get('/rule', async (req, res) => {
	const queryData = url.parse(req.url, true).query;
	const host = encodeURIComponent(req.headers.host);
	const loc = encodeURIComponent(queryData.l);
	const tsoff = encodeURIComponent(queryData.tsoff);
	let queue = encodeURIComponent(queryData.queue);

  	const sSlots=await readFromCache('Slots');
  	const Slots=JSON.parse(sSlots);		

	if (!Number.isInteger(queryData.tsoff*1) || Math.abs(queryData.tsoff*1)>720){
		res.writeHead(503, { 'Content-Type': 'text/javascript' });
		res.end("//offset error");
		return;
	}
	if (!isValidHttpUrl(queryData.l)){
		res.writeHead(503, { 'Content-Type': 'text/javascript' });
		res.end("//location error");
		return;
	}	
    let minutes = (new Date()).getMinutes();
	let hours=(new Date()).getHours();
	let mins=(1440  + minutes*1+hours*60 - queryData.tsoff*1) % 1440 ;

	res.writeHead(200, { 'Content-Type': 'text/javascript' });
    
	
	let slots="";
	if (queue!=null && queue!=="undefined"){
		var t=Slots.SlotQueues[queue];
		var entity=Slots.entities.find(e=>e.key==t);
		if(entity!=null){
		
			let filtered= entity.calculatedSlots.filter(e=>e.availableSlot>e.count).map(item => ({
			   "schedulingTime": item.start,
	                "duration":((new Date(item.end))-(new Date(item.start)))/(1000*60*60)
			}));
			filtered.sort((a, b) => {
    			return a.schedulingTime.localeCompare(b.schedulingTime);
			});			
			slots="\n6;var t="+ JSON.stringify(filtered) + ";\n"+ "try{$setAvailableSchedulingTime(t);}catch(e){}\n";
		}
	}else{
		queue="";
	}
	
	
	let sstats=await readFromCache("stats");
	
	if (sstats!=null){
		// locali per non condividere stato fra richieste concorrenti
		let stats=JSON.parse(sstats);
		let resultChat="";
		var resultCB="";
		if(resultAction(queryData.l,mins,stats))
		{
			resultCB="true";
		}else{
			resultCB="false";
		}
		if(resultActionChat(queryData.l,mins,stats))
		{
			resultChat="true";
		}else{
			resultChat="false";
		}		
		//res.end(slots + `try{$setAvailableSchedulingTime([{"schedulingTime":"` + new Date().toISOString().split('T')[0] + `T09:00:00Z","duration":2},{"schedulingTime":"` + new Date().toISOString().split('T')[0] + `T11:00:00Z","duration":2},{"schedulingTime":"` + new Date(new Date().setDate(new Date().getDate() + 1)).toISOString().split('T')[0] + `T09:00:00Z","duration":2},{"schedulingTime":"` +  new Date(new Date().setDate(new Date().getDate() + 1)).toISOString().split('T')[0] + `T14:00:00Z","duration":2}]);}catch(e){};function webChatPilotAddCustomStyles() {const style = document.createElement('style');style.type = 'text/css';style.innerHTML = ' .genesys-mxg-frame { z-index: 999 !important;} html.no-scroll .genesys-mxg-frame {z-index: 0 !important;}';document.head.appendChild(style);};setTimeout("try{$enableCallMeNow(` + resultCB + `)} catch(e){};try{$enableChat(` + resultChat + `); if(` + resultChat + `){webChatPilotAddCustomStyles();}} catch(e){}",2000);setTimeout(function(){const scriptOSPEElement = document.createElement("script");scriptOSPEElement.src = "https://${host}/rule?l=${loc}&tsoff="+(new Date().getTimezoneOffset());document.head.appendChild(scriptOSPEElement);},60000)`);	
		res.end(slots + `;setTimeout(function(){try{$enableCallMeNow(` + resultCB + `)} catch(e){}},2000);\n` + `setTimeout(function(){const scriptOSPEElement = document.createElement("script");scriptOSPEElement.src = "https://${host}/rule?l=${loc}&queue=${queue}&tsoff="+(new Date().getTimezoneOffset());document.head.appendChild(scriptOSPEElement);},60000);`);			
	}
	else
	{
		//res.end(slots + `try{$setAvailableSchedulingTime([{"schedulingTime":"` + new Date().toISOString().split('T')[0] + `T09:00:00Z","duration":2},{"schedulingTime":"` + new Date().toISOString().split('T')[0] + `T11:00:00Z","duration":2},{"schedulingTime":"` + new Date(new Date().setDate(new Date().getDate() + 1)).toISOString().split('T')[0] + `T09:00:00Z","duration":2},{"schedulingTime":"` + new Date(new Date().setDate(new Date().getDate() + 1)).toISOString().split('T')[0] + `T14:00:00Z","duration":2}]);}catch(e){};setTimeout("try{$enableCallMeNow(false)}catch(e){};try{$enableChat(false)}catch(e){}",2000);setTimeout(function(){const scriptOSPEElement = document.createElement("script");scriptOSPEElement.src = "https://${host}/rule?l=${loc}&tsoff="+(new Date().getTimezoneOffset());document.head.appendChild(scriptOSPEElement);},60000)`);		
		res.end(slots + `;setTimeout(function(){try{$enableCallMeNow(false)} catch(e){}},2000);\n` + `setTimeout(function(){const scriptOSPEElement = document.createElement("script");scriptOSPEElement.src = "https://${host}/rule?l=${loc}&queue=${queue}&tsoff="+(new Date().getTimezoneOffset());document.head.appendChild(scriptOSPEElement);},60000);`);
	}
  
  
  });

	app.use((req, res, next) => {
		res.writeHead(404, { 'Content-Type': 'text/html' });
		res.end("Not found");
	});

  // Creazione del server isolata in httpServer.js: vedi quel file e .snyk
  // (sezione [7]) per la motivazione del listener HTTP in chiaro.
  const server = createHttpServer(app);

  server.listen(8080, () => {
    console.log(`Worker ${process.pid} started and listening on port 8080`);
  });
}

function isValidHttpUrl(string) {
    let url;
    try {
        url = new URL(string);
    } catch (_) {
        return false;
    }
    return url.protocol === "https:";
}

function readFromCache(key) {
    try {
        return redis.get(key);
    } catch (error) {
		return null;
        console.error('Errore durante la lettura dalla cache:'+key, error);
    }
}
async function writeToCache(key, value) {
    try {
        await redis.set(key, value);
        return true;
    } catch (error) {
        console.error('Errore durante la scrittura nel cache:'+key, error);
	return false;
    }
}
function isIsoDate(str) {
    // Validate the format using a regex
    if (!/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z/.test(str)) {
        return false;
    }

    // Parse the date string
    const parsedDate = new Date(str);

    // Check if it's a valid date and matches the original string
    return parsedDate instanceof Date && !isNaN(parsedDate.getTime()) && parsedDate.toISOString() === str;
}

function resultAction(address,minutes,stats){
	address=address.toLowerCase();
	console.log(address);
	if ((address.indexOf("it/cruises.htm")>0 || address.indexOf("it/cruises/")>0 || address.indexOf(".it/offerte.html")>0 || address.indexOf(".it/checkout.")>0 || address.indexOf("/front-end_features/")>0 || address.indexOf(".it/offerte/")>0 || address.indexOf(".it/summary.")>0 || address.indexOf(".it/confirmation.html")>0 || address.indexOf(".it/reservation")>0)){
		let coda="I_CBWB2C";
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + "(min "+minutes+") -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=600 && minutes<=719 && oOnQueueUsers>=5) || (minutes>=720 && minutes<=1079 && oOnQueueUsers>=1) || (minutes>=1080 && minutes<=1139 && oOnQueueUsers>=5)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".fr/promos")>0 || address.indexOf(".fr/cruises.html")>0 || address.indexOf(".fr/cruises/")>0 || address.indexOf(".fr/confirmation.html")>0 || address.indexOf(".fr/summary.")>0 || address.indexOf(".fr/checkout.")>0)){
		let coda="F_CBWB2C";
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=600 && minutes<=719 && oOnQueueUsers>=15) || (minutes>=720 && minutes<=779 && oOnQueueUsers>=25) || (minutes>=900 && minutes<=1079 && oOnQueueUsers>=15)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf("aem-stage2")>0 ) || (address.indexOf("aem-uat2")>0 )){
		
		return true;
		let coda="I_CBWB2CTest";
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			logger.info(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if (oOnQueueUsers>0){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".es/ofertas")>0 || address.indexOf(".es/cruceros.html")>0 || address.indexOf(".es/cruceros/")>0 || address.indexOf(".es/reservation")>0 || address.indexOf("es/summary.")>0 || address.indexOf(".es/confirmation.html")>0 || address.indexOf(".es/checkout.")>0 || address.indexOf(".es/ofertas/promo-super-todo-incluido-2022.html")>0 || address.indexOf(".es/ofertas/ultima-hora.html")>0)){
		let coda="E_CBWB2C";
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=600 && minutes<=779 && oOnQueueUsers>=3) || (minutes>=900 && minutes<=1079 && oOnQueueUsers>=3)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".co.uk/deals")>0 || address.indexOf(".co.uk/cruises.html")>0 || address.indexOf(".co.uk/cruises/")>0 || address.indexOf(".co.uk/reservation.")>0 || address.indexOf("co.uk/summary.")>0 || address.indexOf(".co.uk/checkout.")>0 || address.indexOf(".co.uk/confirmation.html")>0)){
		let coda="GB_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=600 && minutes<=719 && oOnQueueUsers>=2) || (minutes>=900 && minutes<=1079 && oOnQueueUsers>=2)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".ch/fr/promos")>0 || address.indexOf(".ch/fr/cruises.html")>0 || address.indexOf(".ch/fr/cruises/")>0 || address.indexOf(".ch/fr/reservation")>0 || address.indexOf(".ch/fr/confirmation.html")>0 || address.indexOf("ch/fr/summary.")>0 || address.indexOf("ch/fr/checkout.")>0)){
		let coda="CH_F_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=540 && minutes<=1079 && oOnQueueUsers>=2)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".nl/deals")>0 || address.indexOf(".nl/cruises/")>0 || address.indexOf(".nl/cruises.html")>0 || address.indexOf(".nl/reservation")>0 || address.indexOf("nl/summary")>0 || address.indexOf(".nl/confirmation.html")>0 || address.indexOf(".nl/checkout.")>0)){
		let coda="NL_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=600 && minutes<=719 && oOnQueueUsers>=15) || (minutes>=720 && minutes<=779 && oOnQueueUsers>=25) || (minutes>=900 && minutes<=1079 && oOnQueueUsers>=15)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".be/deals")>0 || address.indexOf(".be/cruises/")>0 || address.indexOf(".be/cruises.html")>0 || address.indexOf(".be/reservation")>0 || address.indexOf("be/summary")>0 || address.indexOf(".be/confirmation.html")>0 || address.indexOf(".be/checkout.")>0)){
		let coda="B_F_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=600 && minutes<=719 && oOnQueueUsers>=15) || (minutes>=720 && minutes<=779 && oOnQueueUsers>=25) || (minutes>=900 && minutes<=1079 && oOnQueueUsers>=15)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".fi/deals")>0 || address.indexOf(".fi/cruises/")>0 || address.indexOf(".fi/cruises.html")>0 || address.indexOf(".fi/reservation")>0 || address.indexOf("fi/summary")>0 || address.indexOf(".fi/confirmation.html")>0 || address.indexOf(".fi/checkout.")>0)){
		let coda="SK_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=600 && minutes<=719 && oOnQueueUsers>=15) || (minutes>=720 && minutes<=779 && oOnQueueUsers>=25) || (minutes>=900 && minutes<=1079 && oOnQueueUsers>=15)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".de/angebote")>0 || address.indexOf(".de/cruises/")>0 || address.indexOf(".de/cruises.html")>0 || address.indexOf(".de/reservation")>0 || address.indexOf("de/summary")>0 || address.indexOf(".de/confirmation.html")>0 || address.indexOf(".de/checkout.")>0)){
		let coda="D_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=480 && minutes<=569 && oOnQueueUsers>=4) || (minutes>=570 && minutes<=989 && oOnQueueUsers>=1) || (minutes>=990 && minutes<=1139 && oOnQueueUsers>=5)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".ch/de/angebote")>0 || address.indexOf(".ch/de/cruises/")>0 || address.indexOf(".ch/de/cruises.html")>0 || address.indexOf(".ch/de/reservation")>0 || address.indexOf("ch/de/summary")>0 || address.indexOf(".ch/de/confirmation.html")>0 || address.indexOf(".ch/de/checkout.")>0)){
		let coda="CH_D_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=480 && minutes<=569 && oOnQueueUsers>=4) || (minutes>=570 && minutes<=989 && oOnQueueUsers>=1) || (minutes>=990 && minutes<=1139 && oOnQueueUsers>=5)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf(".at/angebote")>0 || address.indexOf(".at/cruises/")>0 || address.indexOf(".at/cruises.html")>0 || address.indexOf(".at/reservation")>0 || address.indexOf("at/summary")>0 || address.indexOf(".at/confirmation.html")>0 || address.indexOf(".at/checkout.")>0)){
		let coda="A_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=480 && minutes<=569 && oOnQueueUsers>=4) || (minutes>=570 && minutes<=989 && oOnQueueUsers>=1) || (minutes>=990 && minutes<=1139 && oOnQueueUsers>=5)){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf("costacruceros.com/booking/confirmation.htm")>0 )){
		let coda="ARG_CBWOPT";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=540 && minutes<=1139 && oOnQueueUsers>=1) ){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf("costacruceros.co")>0 )){
		let coda="ARG_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=540 && minutes<=1139 && oOnQueueUsers>=1) ){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf("costacruzeiros.com/booking/confirmation.htm")>0 )){
		let coda="BR_CBWOPT";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=540 && minutes<=1139 && oOnQueueUsers>=1) ){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf("costacruzeiros.co")>0 )){
		let coda="BR_CBWB2C";
		
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if ((minutes>=540 && minutes<=1139 && oOnQueueUsers>=1) ){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}
	else
	{
		return false;
	}	
}

function resultActionChat(address,minutes,stats){
	address=address.toLowerCase();
	console.log(address);
	if (address.indexOf("aem-stage2.costacrociere.it")>0  || address.indexOf("aem-uat2.costacrociere.it")>0){
		let coda="I_LiveChatB2CTest";
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			console.log(coda + "(min "+minutes+") -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if (oOnQueueUsers>0){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}else if ((address.indexOf("costacrociere.it")>0 )){
		
		return true;
		let coda="I_LiveChatB2C";
		if (stats.queues[coda]!=null)
		{
			let oOnQueueUsers=stats.queues[coda].oOnQueueUsers;
			let oActiveUsers=stats.queues[coda].oActiveUsers;
			logger.info(coda + " -> oActiveUsers:" + oActiveUsers + " - oOnQueueUsers:" + oOnQueueUsers);
			if (minutes>=540 && minutes<=1139 && oOnQueueUsers>=2){
				return true;
			}
			else
			{
				return false;
			}			
		}
		else
		{
			return false;
		}
	}
	else
	{
		return false;
	}	
}