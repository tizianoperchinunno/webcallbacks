require('dotenv').config();

const Redis = require('ioredis');
const logger = require('./logger');

// Variabili obbligatorie: se mancano, meglio fermarsi subito
const REQUIRED_ENV_VARS = ['REDIS_HOST', 'REDIS_PORT', 'GENESYS_CLIENT_ID', 'GENESYS_CLIENT_SECRET'];
const missingEnvVars = REQUIRED_ENV_VARS.filter((name) => !process.env[name]);
if (missingEnvVars.length > 0) {
    throw new Error('Variabili ambiente mancanti: ' + missingEnvVars.join(', '));
}

// Configura la connessione al tuo cluster ElastiCache
const redis = new Redis({
    host: process.env.REDIS_HOST,
    port: Number(process.env.REDIS_PORT),
    password: process.env.REDIS_PASSWORD || undefined,
    retryStrategy: (times) => {
        return Math.min(times * 1000, 59000); // Ritenta ogni 1000 ms fino a 59 secondi
    },	
});

// clientId per Basic Auth verso l'endpoint OAuth di Genesys Cloud,
// costruito a runtime da client id/secret separati (mai committare il valore gi� codificato)
const clientId = Buffer.from(
    process.env.GENESYS_CLIENT_ID + ':' + process.env.GENESYS_CLIENT_SECRET
).toString('base64');

// fetch e' globale da Node 18: nessuna dipendenza esterna necessaria


function readFromCache(key) {
    try {
        return redis.get(key);
    } catch (error) {
		logger.info("Errore durante la lettura dalla cache: " + JSON.stringify(error));
		return false;
    }
}

async function writeToCache(key, value) {
    try {
        await redis.set(key, value);
        return true;
    } catch (error) {
		logger.info("Errore durante la scrittura dalla cache: " + JSON.stringify(error));
		return false;
    }
}
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Corpo della observations/query: solo i campi previsti dall'API Genesys.
// La mappa id->nome "queues" serve solo a noi e resta nella cache.
function observationQueryBody(filterParam) {
	return JSON.stringify({ filter: filterParam.filter, metrics: filterParam.metrics });
}

async function main(){
    try {
		
		redis.on('error', (err) => {
			logger.info("Errore di connessione a Redis: " + JSON.stringify(err));
		});		

        var bearerToken=await readFromCache('token');
		var sFilterParam=await readFromCache('filter');
		
		var sSlots=await readFromCache('Slots');
		var Slots=JSON.parse(sSlots);
        
        if (bearerToken==null) bearerToken="";
		if (sFilterParam==null) sFilterParam="";
		logger.info("Chiave recuperata da Redis: " + bearerToken);
		//logger.info("Filtro recuperato da Redis: " + sFilterParam);

		const currentDate = new Date();
		var pastDate = new Date();
		pastDate.setDate(currentDate.getDate() - 15);
		
		var conversationFilter={
			"order": "asc",
			"orderBy": "conversationStart",
			"paging": {
				"pageNumber": 1,
				"pageSize": 100
			},
			"interval": pastDate.toISOString() + "/"+ currentDate.toISOString(),
			"segmentFilters": [
				{
					"type": "and",
					"predicates": [
						{
							"dimension": "mediaType",
							"value": "callback"
						},
						{
							"dimension": "segmentType",
							"value": "Scheduled"
						},
						{
							"dimension": "segmentEnd",
							"operator": "notExists"
						}
					]
				},
				{
					"type": "or",
					"predicates": [
						//{
						//	"dimension": "queueId",
						//	"value": "7f93578e-42af-441b-b8b2-7c15aa8fc0ab"
						//}
					]
				}				
			],
			"conversationFilters": [
				{
					"type": "and",
					"predicates": [
						{
							"type": "metric",
							"metric": "tAnswered",
							"operator": "notExists"
						}
					]
				}
			]
		}
		
				
						
		let data = {}
		let conversations = {}
		let stats={timestamp:new Date(),queues:{}}
		let filterParam={}
		
		
		if (bearerToken!="" && sFilterParam!="")
		{
			filterParam=JSON.parse(sFilterParam);
			response = await fetch('https://api.mypurecloud.ie/api/v2/analytics/queues/observations/query', {
				 method: 'POST',
					body: observationQueryBody(filterParam),
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			data = await response.json();
			if (response.status==200){
				logger.info("Statistiche recuperate da API");
			}else{
				logger.info("observations/query in errore (" + response.status + "): " + JSON.stringify(data));
			}
		}

		// Qualsiasi errore (non solo 401) rigenera token e filtro: prima un 400/403/429
		// mandava in crash lo script e le stats in cache restavano vecchie per sempre.
		if (bearerToken=="" || sFilterParam=="" || response.status!=200){
			logger.info("autenticazione non valida");
			response = await fetch('https://login.mypurecloud.ie/oauth/token', {
				 method: 'POST',
					body: 'grant_type=client_credentials',
					headers: { 'Content-Type': 'application/x-www-form-urlencoded','Authorization':'Basic ' + clientId }
			});

			data = await response.json();
			bearerToken=data.access_token;
			logger.info("token recuperato da auth API: " + bearerToken);
			
			if (writeToCache('token', data.access_token)){
				logger.info("token salvato")
			}else{
				logger.info("savlataggio token in errore")
			}


	

			response = await fetch('https://api.mypurecloud.ie/api/v2/flows/datatables/bc0d9fd1-1e37-439c-a416-a45aab740826/rows?showbrief=false&pageSize=200&pageNumber=1&sortOrder=ascending',{
					method: 'GET',
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			let SummerTime = await response.json();

			logger.info("recuperata ora legale");
			
			response = await fetch('https://api.mypurecloud.ie/api/v2/flows/datatables/1e763045-7c2f-47c3-b494-ea43024be0f0/rows?showbrief=false&pageSize=200&pageNumber=1&sortOrder=ascending',{
					method: 'GET',
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			Slots = await response.json();
			
			
			
			var SlotQueues={};	
			
			
			for(c in Slots.entities){
				const q=Slots.entities[c].SelectionQueues.split(",");
				for(cc in q){
					SlotQueues[q[cc]]=Slots.entities[c].key
				}
				let slots=JSON.parse(Slots.entities[c].Slots);
				
				Slots.entities[c].calculatedSlots=[];
				
				let oggi = new Date();
				
				// Ciclo per i prossimi 10 giorni
				for (let i = 0; i < 10; i++) {
				    // Crea una nuova data per ogni iterazione
				    let data = new Date(oggi);
				    
				    // Aggiungi i giorni
				    data.setDate(data.getDate() + i);
				    
				    // Imposta l'orario a mezzogiorno (12:00)
				    data.setHours(12, 0, 0, 0);
				    let summer=SummerTime.entities.find(i=>i.key==Slots.entities[c].Origin+"_"+(data.getFullYear()))
				    // Stampa la data
				    let offset=0;
				    if (data.toISOString()>=summer.StartDate && data.toISOString()<summer.EndDate){
				    
				    	offset=summer.daylight;
				    	//console.log(data.toISOString() + " ora legale")
				    }else{
				    	offset=summer.standard;
				    	//console.log(data.toISOString() + " ora solare")
				    }
				    for (s in slots)
				    {
						let start=data.toISOString().slice(0,11)+("00" + (s.split(":")[0]-offset)).slice(-2)+":"+s.split("/")[0].split(":")[1]+":00.000Z";
						let end=data.toISOString().slice(0,11)+("00" + (s.split("/")[1].split(":")[0]-offset)).slice(-2)+":"+s.split("/")[1].split(":")[1]+":00.000Z";
						Slots.entities[c].calculatedSlots.push({'start':start,'end':end,'availableSlot':slots[s],'count':0})
					}	
				    
				}

			}			
			Slots.SlotQueues=SlotQueues;
			
			
			
						
			var qList="";
			for (a in Slots.entities){
				qList+=Slots.entities[a].Queues + ",";
			}
			
			response = await fetch('https://api.mypurecloud.ie/api/v2/routing/queues?pageSize=500&pageNumber=1&name=*_CBW*',{
					method: 'GET',
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			data = await response.json();
			
			logger.info("recuperate elenco code");
			
			filterParam={ 
				"queues":{},
				"filter": {
					"type": "or",
					"predicates": []
				},
			    "metrics": [
					"oOnQueueUsers",
					"oActiveUsers"
				]
			};
			let queueList={};
			for (i in data.entities){
				filterParam.queues[data.entities[i].id]=data.entities[i].name;
				
				if (qList.indexOf(data.entities[i].name)>=0)
				{
					queueList[data.entities[i].name]=data.entities[i].id;
				}
				
				filterParam.filter.predicates.push({
					"type": "dimension",
					"operator": "matches",
					"value": data.entities[i].id,
					"dimension": "queueId"
				});				
			}
			
			
			Slots.queueList=queueList;
						
			logger.info("recuperati Slot");

			
			//Web chat		
			filterParam.queues["5a43c9a2-5b84-4e7b-aee9-76ceddde1035"]="I_LiveChatB2C";
			filterParam.filter.predicates.push({
				"type": "dimension",
				"operator": "matches",
				"value": "5a43c9a2-5b84-4e7b-aee9-76ceddde1035",
				"dimension": "queueId"
			});	
			filterParam.queues["4d2ff071-d0b8-4d4d-8a4f-ca0d0d6c949e"]="I_LiveChatB2CTest";
			filterParam.filter.predicates.push({
				"type": "dimension",
				"operator": "matches",
				"value": "4d2ff071-d0b8-4d4d-8a4f-ca0d0d6c949e",
				"dimension": "queueId"
			});
			
			sFilterParam=JSON.stringify(filterParam);
			writeToCache('filter', sFilterParam);
			
			logger.info("salvato filtro per observation/query");
			
			response = await fetch('https://api.mypurecloud.ie/api/v2/analytics/queues/observations/query', {
				 method: 'POST',
					body: observationQueryBody(filterParam),
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			data = await response.json();
			//console.log(JSON.stringify(data));
			if (response.status!=200){
				throw new Error("observations/query in errore (" + response.status + "): " + JSON.stringify(data));
			}
			logger.info("statistiche recuperate");



		}



		// Genesys omette "results" e "data" quando non ci sono osservazioni, e una
		// stessa coda puo' comparire in piu' gruppi (es. per mediaType): i conteggi
		// vanno sommati, non sovrascritti, altrimenti un gruppo vuoto azzera la coda.
		const results = Array.isArray(data.results) ? data.results : [];
		for (const result of results){
			const queueName = filterParam.queues[result.group && result.group.queueId];
			if (queueName == null) continue;
			if (stats.queues[queueName] == null){
				stats.queues[queueName] = { oOnQueueUsers: 0, oActiveUsers: 0 };
			}
			for (const d of (result.data || [])){
				const count = (d.stats && d.stats.count) || 0;
				if (d.metric=='oOnQueueUsers')
				{
					stats.queues[queueName].oOnQueueUsers+=count;
				}
				if (d.metric=='oActiveUsers')
				{
					stats.queues[queueName].oActiveUsers+=count;
				}
			}
		}
		stats.timestamp=new Date();
		writeToCache('stats', JSON.stringify(stats));
		logger.info("statistiche salvate in cache (" + Object.keys(stats.queues).length + " code)");
		//console.log(JSON.stringify(stats));




		let pageNumber=0;
		let convCount=101;
		let conteggi=[];
		

		for(q in Slots.queueList){
			conversationFilter.segmentFilters[1].predicates.push({"dimension":"queueId","value": Slots.queueList[q]})
		}
				
		while (convCount>pageNumber*100)
		{
			conversationFilter.paging.pageNumber=pageNumber+1;
			
			//aggiunta code
			
			
			
			let sConversationFilter=JSON.stringify(conversationFilter);		
			//console.log(sConversationFilter);
			
			response = await fetch('https://api.mypurecloud.ie/api/v2/analytics/conversations/details/query', {
				 method: 'POST',
					body: sConversationFilter,
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});		
			conversations = await response.json();
			if (response.status!=200){
				throw new Error("conversations/details/query in errore (" + response.status + "): " + JSON.stringify(conversations));
			}

			//.filter(conve=>conve.conversationId=='cc313aa7-7060-4d6a-953e-5d1e7b9cf5cb')
			// senza callback pendenti Genesys risponde solo {"totalHits":0}, senza "conversations"
			conteggi=conteggi.concat((conversations.conversations || []).filter(conve=>Slots.SlotQueues[filterParam.queues[conve.participants[0].sessions[0].segments[0].queueId]]!=null).filter(conv => (conv.participants[0].purpose=="customer" && conv.participants[0].participantName.indexOf("Default")<0) || (conv.participants[1].purpose=="customer" && conv.participants[1].participantName.indexOf("Default")<0) )
			.map(item => ({
  				//queueName: filterParam.queues[item.participants[0].sessions[0].segments[0].queueId],
  				slotName: Slots.SlotQueues[filterParam.queues[item.participants[0].sessions[0].segments[0].queueId]],
  				conversationId: item.conversationId,
  				originalcallbackScheduledTime: item.participants[0].sessions[0].callbackScheduledTime,
				callbackScheduledTime: Slots.entities.find(ss=> ss.key==Slots.SlotQueues[filterParam.queues[item.participants[0].sessions[0].segments[0].queueId]]).calculatedSlots.find(dt => (dt.start<=item.participants[0].sessions[0].callbackScheduledTime) && (dt.end>item.participants[0].sessions[0].callbackScheduledTime))
			})));
			conteggi=conteggi.filter(conve=>conve.callbackScheduledTime!=null)
			await delay(1000);
			

			
			
			if (conversations.totalHits)
			{
				convCount=conversations.totalHits
				pageNumber+=1;
			}else{
				convCount=0;
			}
		}
		
		//console.log(conteggi);
		
		for (sl in Slots.entities){
			for (csl in Slots.entities[sl].calculatedSlots){
				Slots.entities[sl].calculatedSlots[csl].count=0;
			}
		}
		
		const occurrences = conteggi.reduce((acc, item) => {
  			const key = `${item.slotName}>${item.callbackScheduledTime.start}`;
  			acc[key] = (acc[key] || 0) + 1;
  			return acc;
		}, {});
		//console.log(occurrences);
		
		for (o in occurrences){
			Slots.entities.find(occ=>occ.key==o.split(">")[0]).calculatedSlots.find(cs=>cs.start==o.split(">")[1]).count=occurrences[o];
		}
		
		Slots.timestamp=(new Date()).toISOString()
		writeToCache('Slots', JSON.stringify(Slots));
		
	
		logger.info("Statistiche recuperate da API");


    } catch (error) {
		// tramite logger, cosi' l'errore finisce anche nel file di log e non solo su stdout
		logger.error('Errore durante l\'estrazione delle statistiche: ' + ((error && error.stack) || error));
    }

	redis.quit();
}

main()