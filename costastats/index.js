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
    // senza REDIS_PASSWORD si connette senza AUTH, come prima
    password: process.env.REDIS_PASSWORD || undefined,
    retryStrategy: (times) => {
        return Math.min(times * 1000, 59000); // Ritenta ogni 1000 ms fino a 59 secondi
    },	
});

// clientId per Basic Auth verso l'endpoint OAuth di Genesys Cloud,
// costruito a runtime da client id/secret separati (mai committare il valore codificato)
const clientId = Buffer.from(
    process.env.GENESYS_CLIENT_ID + ':' + process.env.GENESYS_CLIENT_SECRET
).toString('base64');

// fetch e' globale da Node 18: nessuna dipendenza esterna necessaria

// Mai scrivere nei log il token completo (i log restano 20 giorni e il token
// da' accesso alle API Genesys): bastano le ultime cifre per riconoscerlo
function mascheraToken(token) {
	return token ? "..." + String(token).slice(-6) : "(assente)";
}

// Legge la risposta di una chiamata Genesys e la rifiuta se non e' un 200:
// durante la rigenerazione una risposta di errore (429, 5xx...) letta come
// buona finiva in cache (filtro senza code, slot vuoti) e ci restava fino al
// rinnovo successivo del token, cioe' fino a un giorno
async function leggiRisposta(response, nome) {
	const body = await response.json().catch(() => null);
	if (response.status != 200 || body == null) {
		throw new Error(nome + " in errore (" + response.status + "): " + JSON.stringify(body));
	}
	return body;
}


// Un errore di Redis (Promise rifiutata) arriva al catch di main(), che lo logga
// e chiude l'esecuzione: senza cache non ha senso chiamare Genesys. Il try/catch
// che c'era qui non intercettava nulla (mancava await) e restituiva un false mai usato
function readFromCache(key) {
    return redis.get(key);
}

async function writeToCache(key, value) {
    try {
        await redis.set(key, value);
        return true;
    } catch (error) {
		logger.error("Errore durante la scrittura nella cache: " + key + " " + ((error && error.message) || error));
		return false;
    }
}

// Oltre questo tempo una chiamata a Genesys si considera fallita: senza limite
// un'esecuzione bloccata poteva sovrapporsi a quelle successive
const GENESYS_TIMEOUT_MS = 30000;
function chiamaGenesys(url, options) {
	return fetch(url, Object.assign({ signal: AbortSignal.timeout(GENESYS_TIMEOUT_MS) }, options));
}

// Converte un orario locale "HH:MM" del giorno di riferimento in UTC.
// Prima l'ora si calcolava come testo ("00"+(ora-offset)): fuori dalla giornata
// UTC usciva un orario non valido, es. 20:00 a UTC-3 -> "T25:00", 00:30 a UTC+1 -> "T-1:30"
function orarioSlotUtc(giorno, hhmm, offset) {
	const [ore, minuti] = hhmm.split(":");
	// tutto in minuti, cosi' funzionano anche gli offset di mezz'ora (es. 5.5)
	const minutiUtc = Number(ore) * 60 + Number(minuti) - Number(offset) * 60;
	const d = new Date(Date.UTC(giorno.getUTCFullYear(), giorno.getUTCMonth(), giorno.getUTCDate(), 0, minutiUtc));
	return d.toISOString(); // RangeError se l'orario o l'offset non sono validi
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
    // dichiarata qui: prima era una variabile globale implicita
    let response;
    try {

		redis.on('error', (err) => {
			logger.error("Errore di connessione a Redis: " + ((err && err.message) || err));
		});

        var bearerToken=await readFromCache('token');
		var sFilterParam=await readFromCache('filter');
		
		var sSlots=await readFromCache('Slots');
		var Slots=sSlots ? JSON.parse(sSlots) : null;

        if (bearerToken==null) bearerToken="";
		if (sFilterParam==null) sFilterParam="";
		logger.info("Chiave recuperata da Redis: " + mascheraToken(bearerToken));
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
			response = await chiamaGenesys('https://api.mypurecloud.ie/api/v2/analytics/queues/observations/query', {
				 method: 'POST',
					body: observationQueryBody(filterParam),
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			data = await response.json().catch(() => ({}));
			if (response.status==200){
				logger.info("Statistiche recuperate da API");
			}else{
				logger.info("observations/query in errore (" + response.status + "): " + JSON.stringify(data));
			}
		}

		// Qualsiasi errore (non solo 401) rigenera token e filtro: prima un 400/403/429
		// mandava in crash lo script e le stats in cache restavano vecchie per sempre.
		// Anche senza la chiave Slots in cache: prima, con token e filtro validi, ogni
		// esecuzione si fermava su Slots.queueList (null) fino alla scadenza del token
		if (bearerToken=="" || sFilterParam=="" || response.status!=200 || Slots==null){
			logger.info("autenticazione non valida");
			response = await chiamaGenesys('https://login.mypurecloud.ie/oauth/token', {
				 method: 'POST',
					body: 'grant_type=client_credentials',
					headers: { 'Content-Type': 'application/x-www-form-urlencoded','Authorization':'Basic ' + clientId }
			});

			data = await leggiRisposta(response, "oauth/token");
			if (!data.access_token) throw new Error("oauth/token senza access_token");
			bearerToken=data.access_token;
			logger.info("token recuperato da auth API: " + mascheraToken(bearerToken));

			// await: senza, writeToCache restituisce una Promise (sempre "vera")
			if (await writeToCache('token', data.access_token)){
				logger.info("token salvato")
			}else{
				logger.info("savlataggio token in errore")
			}


	

			response = await chiamaGenesys('https://api.mypurecloud.ie/api/v2/flows/datatables/bc0d9fd1-1e37-439c-a416-a45aab740826/rows?showbrief=false&pageSize=200&pageNumber=1&sortOrder=ascending',{
					method: 'GET',
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			let SummerTime = await leggiRisposta(response, "tabella ora legale");
			if (!Array.isArray(SummerTime.entities)) SummerTime.entities=[];

			logger.info("recuperata ora legale");

			response = await chiamaGenesys('https://api.mypurecloud.ie/api/v2/flows/datatables/1e763045-7c2f-47c3-b494-ea43024be0f0/rows?showbrief=false&pageSize=200&pageNumber=1&sortOrder=ascending',{
					method: 'GET',
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			Slots = await leggiRisposta(response, "tabella slot");
			if (!Array.isArray(Slots.entities)) throw new Error("tabella slot senza righe: " + JSON.stringify(Slots));



			var SlotQueues={};
			const righeOraLegaleMancanti=new Set();
			const fasceNonValide=new Set();


			for (const c in Slots.entities){
				Slots.entities[c].calculatedSlots=[];
				// campo vuoto o con spazi ("A, B"): prima un campo vuoto bloccava tutto il calcolo
				const q=String(Slots.entities[c].SelectionQueues || "").split(",").map(x=>x.trim()).filter(x=>x!="");
				for (const cc in q){
					SlotQueues[q[cc]]=Slots.entities[c].key
				}
				let slots;
				try {
					slots=JSON.parse(Slots.entities[c].Slots);
				} catch (e) {
					// una riga con Slots non valido non deve bloccare il calcolo delle altre
					logger.error("tabella slot: campo Slots non valido nella riga " + Slots.entities[c].key + ": " + Slots.entities[c].Slots);
					continue;
				}

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
				    if (summer==null){
				    	// manca la riga dell'anno (es. a fine dicembre i 10 giorni sconfinano
				    	// nell'anno dopo): si salta il giorno invece di bloccare tutti gli slot
				    	righeOraLegaleMancanti.add(Slots.entities[c].Origin+"_"+(data.getFullYear()));
				    	continue;
				    }
				    // Stampa la data
				    let offset=0;
				    if (data.toISOString()>=summer.StartDate && data.toISOString()<summer.EndDate){
				    
				    	offset=summer.daylight;
				    	//console.log(data.toISOString() + " ora legale")
				    }else{
				    	offset=summer.standard;
				    	//console.log(data.toISOString() + " ora solare")
				    }
				    for (const s in slots)
				    {
						// fascia "HH:MM/HH:MM": se e' scritta male si salta solo quella
						// (prima un TypeError fermava il calcolo di tutti gli slot)
						let start, end;
						try {
							const [inizio, fine] = s.split("/");
							start=orarioSlotUtc(data, inizio, offset);
							end=orarioSlotUtc(data, fine, offset);
						} catch (e) {
							fasceNonValide.add(Slots.entities[c].key + " " + s);
							continue;
						}
						Slots.entities[c].calculatedSlots.push({'start':start,'end':end,'availableSlot':slots[s],'count':0})
					}
				    
				}

			}
			if (righeOraLegaleMancanti.size>0){
				logger.error("tabella ora legale: mancano le righe " + Array.from(righeOraLegaleMancanti).join(", ") + " (quei giorni restano senza slot)");
			}
			if (fasceNonValide.size>0){
				logger.error("tabella slot: fasce orarie o offset non validi, saltati: " + Array.from(fasceNonValide).join(", "));
			}
			Slots.SlotQueues=SlotQueues;
			
			
			
						
			var qList="";
			for (const a in Slots.entities){
				qList+=Slots.entities[a].Queues + ",";
			}
			
			response = await chiamaGenesys('https://api.mypurecloud.ie/api/v2/routing/queues?pageSize=500&pageNumber=1&name=*_CBW*',{
					method: 'GET',
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			data = await leggiRisposta(response, "routing/queues");
			if (!Array.isArray(data.entities) || data.entities.length==0){
				throw new Error("routing/queues: nessuna coda *_CBW* restituita");
			}

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
			for (const i in data.entities){
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

			response = await chiamaGenesys('https://api.mypurecloud.ie/api/v2/analytics/queues/observations/query', {
				 method: 'POST',
					body: observationQueryBody(filterParam),
					headers: { 'Content-Type': 'application/json','Authorization':'Bearer '+ bearerToken}
			});
			//console.log(JSON.stringify(data));
			data = await leggiRisposta(response, "observations/query");
			logger.info("statistiche recuperate");

			// il filtro si salva solo dopo che Genesys lo ha accettato: se qualcosa
			// va storto resta in cache l'ultimo filtro buono
			await writeToCache('filter', sFilterParam);
			logger.info("salvato filtro per observation/query");



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
		await writeToCache('stats', JSON.stringify(stats));
		logger.info("statistiche salvate in cache (" + Object.keys(stats.queues).length + " code)");
		//console.log(JSON.stringify(stats));




		let pageNumber=0;
		let convCount=101;
		let conteggi=[];
		

		for (const q in Slots.queueList){
			conversationFilter.segmentFilters[1].predicates.push({"dimension":"queueId","value": Slots.queueList[q]})
		}

		// participants/sessions/segments o participantName possono mancare (es. callback
		// senza nome): prima un solo caso del genere bloccava il conteggio di tutti gli slot
		const primaSessione = (conv) => conv.participants?.[0]?.sessions?.[0];
		const slotDi = (conv) => Slots.SlotQueues[filterParam.queues[primaSessione(conv)?.segments?.[0]?.queueId]];
		const isCliente = (p) => p?.purpose=="customer" && (p.participantName || "").indexOf("Default")<0;

		while (convCount>pageNumber*100)
		{
			conversationFilter.paging.pageNumber=pageNumber+1;
			
			//aggiunta code
			
			
			
			let sConversationFilter=JSON.stringify(conversationFilter);		
			//console.log(sConversationFilter);
			
			response = await chiamaGenesys('https://api.mypurecloud.ie/api/v2/analytics/conversations/details/query', {
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
			conteggi=conteggi.concat((conversations.conversations || []).filter(conve=>slotDi(conve)!=null).filter(conv => isCliente(conv.participants[0]) || isCliente(conv.participants[1]))
			.map(item => ({
  				//queueName: filterParam.queues[item.participants[0].sessions[0].segments[0].queueId],
  				slotName: slotDi(item),
  				conversationId: item.conversationId,
  				originalcallbackScheduledTime: primaSessione(item).callbackScheduledTime,
				callbackScheduledTime: (Slots.entities.find(ss=> ss.key==slotDi(item))?.calculatedSlots || []).find(dt => (dt.start<=primaSessione(item).callbackScheduledTime) && (dt.end>primaSessione(item).callbackScheduledTime))
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
		
		for (const sl in Slots.entities){
			for (const csl in Slots.entities[sl].calculatedSlots){
				Slots.entities[sl].calculatedSlots[csl].count=0;
			}
		}
		
		const occurrences = conteggi.reduce((acc, item) => {
  			const key = `${item.slotName}>${item.callbackScheduledTime.start}`;
  			acc[key] = (acc[key] || 0) + 1;
  			return acc;
		}, {});
		//console.log(occurrences);
		
		for (const o in occurrences){
			Slots.entities.find(occ=>occ.key==o.split(">")[0]).calculatedSlots.find(cs=>cs.start==o.split(">")[1]).count=occurrences[o];
		}
		
		Slots.timestamp=(new Date()).toISOString()
		await writeToCache('Slots', JSON.stringify(Slots));
		
	
		logger.info("Statistiche recuperate da API");


    } catch (error) {
		// tramite logger, cosi' l'errore finisce anche nel file di log e non solo su stdout
		logger.error('Errore durante l\'estrazione delle statistiche: ' + ((error && error.stack) || error));
    }

	redis.quit();
}

main()