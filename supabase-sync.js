"use strict";

/* ============================================================================
   FLOWSIM FINANCIERO · SINCRONIZACIÓN TOTAL CON SUPABASE V2
   - Conserva el estado completo serializable del simulador.
   - Normaliza personas, pasos, eventos, movimientos, créditos, cartera,
     escenarios, nodos, conexiones, runtime y métricas para Pentaho.
   - Todos los importes monetarios se redondean a 2 decimales y se envían como USD.
   - Usa una cola persistente en IndexedDB para reintentar cuando falle Internet.
   ============================================================================ */
const supabaseSync = (() => {
  const config = window.SUPABASE_CONFIG || {};
  const projectUrl = String(config.url || "").replace(/\/+$/, "");
  const publishableKey = String(config.publishableKey || "").trim();
  const rpcUrl = projectUrl ? `${projectUrl}/rest/v1/rpc/guardar_lote_simulador` : "";
  const syncIntervalMs = Math.max(1000, Number(config.syncIntervalMs) || 3000);
  const fullSnapshotIntervalMs = Math.max(5000, Number(config.fullSnapshotIntervalMs) || 15000);
  const maxEventsPerSync = Math.max(500, Number(config.maxEventsPerSync) || 5000);
  const requestTimeoutMs = Math.max(5000, Number(config.requestTimeoutMs) || 30000);

  const SESSION_KEY = "flowsimSupabaseSessionV2";
  const OUTBOX_DB = "flowsimSupabaseOutboxV2";
  const OUTBOX_STORE = "outbox";
  const OUTBOX_FALLBACK_PREFIX = "flowsimSupabaseOutboxV2:";

  let runId = "";
  let startedAt = "";
  let endedAt = null;
  let runStatus = "sin_iniciar";
  let active = false;
  let initialized = false;
  let syncing = false;
  let syncRequested = false;
  let timer = null;
  let lastFullSnapshotAt = 0;
  let lastMetricKey = "";

  const personSignatures = new Map();
  const stepCounts = new Map();
  const timelineCounts = new Map();
  const movementKeys = new Set();
  const creditSignatures = new Map();
  const loanSignatures = new Map();
  const variableSignatures = new Map();

  function isConfigured() {
    return /^https:\/\/[a-z0-9-]+\.supabase\.co$/i.test(projectUrl)
      && publishableKey.length > 20
      && !publishableKey.includes("PEGUE_AQUI");
  }

  function createUuid() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, character => {
      const random = Math.random() * 16 | 0;
      const value = character === "x" ? random : (random & 0x3 | 0x8);
      return value.toString(16);
    });
  }

  function roundMoney(value) {
    if (value === "" || value === null || value === undefined) return null;
    const number = Number(value);
    if (!Number.isFinite(number)) return null;
    const sign = number < 0 ? -1 : 1;
    return Math.round((number + sign * Number.EPSILON) * 100) / 100;
  }

  function moneyOrZero(value) {
    const result = roundMoney(value);
    return result === null ? 0 : result;
  }


  const TRANSACTION_AMOUNT_LIMITS = Object.freeze({
    credit: { min: 200, max: 200000 },
    deposit: { min: 1, max: 5000 },
    withdrawal: { min: 1, max: 3000 },
    transfer: { min: 1, max: 15000 },
    payment: { min: 1, max: 1500 },
    inquiry: { min: 0, max: 0 },
    account: { min: 0, max: 500 },
    card: { min: 0, max: 200 },
    insurance: { min: 5, max: 1000 },
    investment: { min: 50, max: 25000 }
  });

  const CREDIT_AMOUNT_LIMITS = Object.freeze({
    housing: { min: 10000, max: 200000 },
    vehicle: { min: 3000, max: 80000 },
    consumer: { min: 200, max: 40000 },
    business: { min: 500, max: 150000 },
    education: { min: 300, max: 50000 }
  });

  function amountLimit(type, creditType = "") {
    if (type === "credit" && CREDIT_AMOUNT_LIMITS[creditType]) return CREDIT_AMOUNT_LIMITS[creditType];
    return TRANSACTION_AMOUNT_LIMITS[type] || { min: 0, max: 200000 };
  }

  function safeTransactionAmount(type, value, creditType = "", nullable = true) {
    if ((value === "" || value === null || value === undefined) && nullable) return null;
    const number = Number(value);
    if (!Number.isFinite(number)) return nullable ? null : 0;
    const limit = amountLimit(type, creditType);
    return roundMoney(Math.min(limit.max, Math.max(limit.min, number)));
  }

  function inferTransactionType(movementType, fallback = "") {
    const text = String(movementType || fallback || "").toLowerCase();
    if (text.includes("credit") || text.includes("crédito") || text.includes("desembolso")) return "credit";
    if (text.includes("deposit") || text.includes("depósito")) return "deposit";
    if (text.includes("withdraw") || text.includes("retiro")) return "withdrawal";
    if (text.includes("transfer")) return "transfer";
    if (text.includes("payment") || text.includes("pago")) return "payment";
    if (text.includes("insurance") || text.includes("seguro")) return "insurance";
    if (text.includes("investment") || text.includes("inversión")) return "investment";
    return fallback || "";
  }

  function safeMovementEffect(movementType, transactionType, value, creditType = "") {
    const number = Number(value);
    if (!Number.isFinite(number)) return 0;
    const type = inferTransactionType(movementType, transactionType);
    const limit = amountLimit(type || "credit", creditType);
    const magnitude = Math.min(limit.max, Math.max(0, Math.abs(number)));
    return roundMoney(number < 0 ? -magnitude : magnitude);
  }

  function roundNumber(value, decimals = 3) {
    if (value === "" || value === null || value === undefined) return null;
    const number = Number(value);
    return Number.isFinite(number) ? Number(number.toFixed(decimals)) : null;
  }

  function numberOrZero(value, decimals = 3) {
    const result = roundNumber(value, decimals);
    return result === null ? 0 : result;
  }

  function stableSignature(value) {
    try { return JSON.stringify(value); } catch (error) { return `${Date.now()}-${Math.random()}`; }
  }

  function setConnectionStatus(message, type = "") {
    const element = document.getElementById("supabaseStatus");
    if (!element) return;
    element.textContent = message;
    element.classList.remove("connected", "syncing", "error");
    if (type) element.classList.add(type);
  }

  function persistSession() {
    try {
      window.localStorage.setItem(SESSION_KEY, JSON.stringify({
        runId,
        startedAt,
        endedAt,
        runStatus,
        active,
        savedAt: new Date().toISOString()
      }));
    } catch (error) {
      console.warn("No se pudo conservar la sesión de Supabase:", error);
    }
  }

  function restoreSession() {
    try {
      const raw = window.localStorage.getItem(SESSION_KEY);
      if (!raw) return;
      const session = JSON.parse(raw);
      if (!session || typeof session !== "object") return;
      runId = String(session.runId || "");
      startedAt = String(session.startedAt || "");
      endedAt = session.endedAt || null;
      runStatus = String(session.runStatus || "sin_iniciar");
      active = Boolean(session.active);
    } catch (error) {
      console.warn("No se pudo restaurar la sesión de Supabase:", error);
    }
  }

  function clearSession() {
    try { window.localStorage.removeItem(SESSION_KEY); } catch (error) {}
  }

  function simulationIsoDateTime(simMinute = state.simTime) {
    const minute = Number.isFinite(Number(simMinute)) ? Number(simMinute) : Number(state.simTime || 0);
    const date = simulationDate(minute);
    const [hours, minutes] = simulationClock(minute).split(":").map(Number);
    const local = new Date(
      date.getFullYear(),
      date.getMonth(),
      date.getDate(),
      Number.isFinite(hours) ? hours : 0,
      Number.isFinite(minutes) ? minutes : 0,
      0,
      0
    );
    return local.toISOString();
  }

  function isRateOrRatioPath(path) {
    return /(?:rate|ratio|pct|percent|probability|probabilidad|weight|peso|share|tasa|annualinterest|annualrate|monthlydefault|commissionrate|recoveryrate|appointmentrate|priorityrate|digitalshare|outagerate|maxdebtratio)/i.test(path);
  }

  function isUsdPath(path) {
    const normalizedPath = String(path || "").replace(/\[(\d+)\]/g, ".$1");
    const lower = normalizedPath.toLowerCase();
    if (!lower || isRateOrRatioPath(lower)) return false;

    const rangeContext = /(automaticamountranges|creditamountranges|default_automatic_amount|default_credit_amount|transaction_types|credit_types|automaticamountprofiles)/i.test(lower);
    if (rangeContext && /(?:\.min|\.max|\.amount|\.minamount|\.maxamount)$/i.test(lower)) return true;

    const moneyKeys = new Set([
      "initialcapital", "currentcapital", "minimumreserve", "initialvaultcash", "initialcashdesk", "amlthreshold",
      "cashdesk", "vaultcash", "clientdeposits", "investmentliability", "loanportfolio", "overdueportfolio",
      "recoveredtotal", "commissionincome", "interestcollected", "staffcost", "operatingcost",
      "amountusd", "amountfixed", "amountmin", "amountmax", "staffcostperhour", "cashminimum", "cashmaximum",
      "monthlyincome", "currentmonthlydebt", "creditminincome", "creditmonthlypayment", "credittotalinterest", "credittotalpayable", "totalinterest", "totalpayable",
      "capitaleffect", "capitalbefore", "capitalafter", "principal", "outstandingprincipal", "monthlypayment", "overdueamount",
      "amount", "before", "after", "effect", "equity", "capital", "recovered", "recovery", "interestpaid",
      "principalpaid", "expectedpayment", "commission", "interestreceivable", "loanpaymentsreceived",
      "deposittotal", "withdrawaltotal", "creditdisbursed", "paymenttotal", "transferintotal", "transferouttotal"
    ]);
    const segments = lower.split(".").filter(Boolean);
    return segments.some(segment => moneyKeys.has(segment));
  }

  function normalizeSerializable(value, path = "", seen = new WeakSet()) {
    if (value === undefined) return null;
    if (value === null) return null;
    if (typeof value === "number") {
      if (!Number.isFinite(value)) return null;
      return isUsdPath(path) ? moneyOrZero(value) : Number(value);
    }
    if (typeof value === "string" || typeof value === "boolean") return value;
    if (typeof value === "bigint") return value.toString();
    if (typeof value === "function" || typeof value === "symbol") return null;
    if (value instanceof Date) return value.toISOString();
    if (value instanceof Map) {
      return Array.from(value.entries()).map(([key, item], index) => [
        normalizeSerializable(key, `${path}[${index}].key`, seen),
        normalizeSerializable(item, `${path}[${index}].value`, seen)
      ]);
    }
    if (value instanceof Set) {
      return Array.from(value.values()).map((item, index) => normalizeSerializable(item, `${path}[${index}]`, seen));
    }
    if (typeof Element !== "undefined" && value instanceof Element) {
      return {
        tag: value.tagName,
        id: value.id || null,
        class_name: value.className || null,
        title: value.title || null,
        data_code: value.dataset?.code || null,
        left: value.style?.left || null,
        top: value.style?.top || null
      };
    }
    if (typeof value === "object") {
      if (seen.has(value)) return "[Referencia circular]";
      seen.add(value);
      if (Array.isArray(value)) {
        const result = value.map((item, index) => normalizeSerializable(item, `${path}[${index}]`, seen));
        seen.delete(value);
        return result;
      }
      const output = {};
      Object.keys(value).forEach(key => {
        output[key] = normalizeSerializable(value[key], path ? `${path}.${key}` : key, seen);
      });
      seen.delete(value);
      return output;
    }
    return String(value);
  }

  function serializeNodeRuntime() {
    return Array.from(state.nodeRuntime.entries()).map(([nodeId, runtime]) => ({
      nodo_id: String(nodeId),
      ingresados: Number(runtime.entered || 0),
      completados: Number(runtime.completed || 0),
      rechazados: Number(runtime.rejected || 0),
      ocupados: Number(runtime.busy || 0),
      longitud_cola: Array.isArray(runtime.queue) ? runtime.queue.length : 0,
      personas_cola: Array.isArray(runtime.queue)
        ? runtime.queue.map(entity => entity?.label || null).filter(Boolean)
        : [],
      datos_adicionales: normalizeSerializable(runtime, `state.nodeRuntime.${nodeId}`)
    }));
  }

  function serializeScheduledEvents() {
    return state.events.map(event => ({
      id: event.id,
      time: roundNumber(event.time, 3),
      type: event.type || null,
      node_id: event.payload?.nodeId || null,
      persona_codigo: event.payload?.entity?.label || null,
      payload: normalizeSerializable(event.payload, `state.events.${event.id}.payload`)
    }));
  }

  function serializeMovingEntities() {
    return state.movingEntities.map((move, index) => ({
      persona_codigo: move.entity?.label || null,
      entidad: normalizeSerializable(move.entity, `state.movingEntities[${index}].entity`),
      destino_nodo_id: move.toNodeId || null,
      inicio_minuto: roundNumber(move.startTime, 3),
      duracion_minutos: roundNumber(move.duration, 3),
      elemento: normalizeSerializable(move.element, `state.movingEntities[${index}].element`)
    }));
  }

  function buildDefinitions() {
    return normalizeSerializable({
      TRANSACTION_TYPES,
      CREDIT_TYPES,
      DEFAULT_AUTOMATIC_AMOUNT_PROFILES,
      DEFAULT_AUTOMATIC_AMOUNT_RANGES,
      DEFAULT_CREDIT_AMOUNT_RANGES,
      NODE_TYPES
    }, "definitions");
  }

  function buildRuntimeState() {
    return normalizeSerializable({
      currentSimulationDay: state.currentSimulationDay,
      selected: state.selected,
      connectionMode: state.connectionMode,
      connectionOrigin: state.connectionOrigin,
      zoom: state.zoom,
      draggingNode: state.draggingNode,
      dragOffset: state.dragOffset,
      dragStart: state.dragStart,
      dragMoved: state.dragMoved,
      dragPointerId: state.dragPointerId,
      running: state.running,
      paused: state.paused,
      simTime: state.simTime,
      speed: state.speed,
      lastFrame: state.lastFrame,
      nextEntityId: state.nextEntityId,
      nextEventId: state.nextEventId,
      nextHistoryTime: state.nextHistoryTime,
      sourceSchedule: Array.from(state.sourceSchedule.entries()),
      nodeRuntime: serializeNodeRuntime(),
      scheduledEvents: serializeScheduledEvents(),
      movingEntities: serializeMovingEntities()
    }, "state.runtime");
  }

  function buildCompleteState() {
    return normalizeSerializable({
      nodes: state.nodes,
      edges: state.edges,
      records: state.records,
      capitalLedger: state.capitalLedger,
      scenario: state.scenario,
      financial: state.financial,
      loanAccounts: state.loanAccounts,
      scenarioSnapshots: state.scenarioSnapshots,
      currentSimulationDay: state.currentSimulationDay,
      operationalEvent: state.operationalEvent,
      selected: state.selected,
      connectionMode: state.connectionMode,
      connectionOrigin: state.connectionOrigin,
      zoom: state.zoom,
      draggingNode: state.draggingNode,
      dragOffset: state.dragOffset,
      dragStart: state.dragStart,
      dragMoved: state.dragMoved,
      dragPointerId: state.dragPointerId,
      running: state.running,
      paused: state.paused,
      simTime: state.simTime,
      speed: state.speed,
      lastFrame: state.lastFrame,
      nextEntityId: state.nextEntityId,
      nextEventId: state.nextEventId,
      events: serializeScheduledEvents(),
      movingEntities: serializeMovingEntities(),
      nodeRuntime: serializeNodeRuntime(),
      sourceSchedule: Array.from(state.sourceSchedule.entries()),
      metrics: state.metrics,
      nextHistoryTime: state.nextHistoryTime,
      definitions: buildDefinitions()
    }, "state");
  }

  function collectEntityMap() {
    const entities = new Map();
    const add = entity => {
      if (entity && entity.label) entities.set(String(entity.label), entity);
    };
    state.movingEntities.forEach(move => add(move.entity));
    state.events.forEach(event => add(event.payload?.entity));
    state.nodeRuntime.forEach(runtime => {
      if (Array.isArray(runtime.queue)) runtime.queue.forEach(add);
    });
    return entities;
  }

  function collectRecordMap() {
    return new Map(state.records.map(record => [String(record.personId), record]));
  }

  function findEntityQueueNode(entityCode) {
    let nodeId = null;
    state.nodeRuntime.forEach((runtime, currentNodeId) => {
      if (nodeId || !Array.isArray(runtime.queue)) return;
      if (runtime.queue.some(entity => entity?.label === entityCode)) nodeId = currentNodeId;
    });
    return nodeId;
  }

  function personRowFromEntity(entity) {
    const profile = entity.creditProfile || null;
    const queueNodeId = findEntityQueueNode(entity.label);
    return {
      persona_codigo: String(entity.label),
      tipo_usuario: entity.personType || "Ciudadano",
      con_cita: Boolean(entity.hasAppointment),
      prioridad: entity.priorityLevel === "priority" ? "Prioritario" : "Regular",
      canal: entity.serviceChannel || "Presencial",
      revision_cumplimiento: Boolean(entity.complianceFlag),
      satisfaccion_1_5: null,
      categoria_nps: null,
      tramite_tipo: entity.transactionType || null,
      tramite_nombre: entity.transactionLabel || null,
      tramite_codigo: entity.transactionCode || null,
      monto_usd: safeTransactionAmount(entity.transactionType, entity.amountUsd, entity.creditType),
      tipo_credito: entity.creditType || null,
      tipo_credito_nombre: entity.creditTypeLabel || null,
      fecha_ingreso: simulationIsoDateTime(entity.createdAt),
      fecha_salida: null,
      minuto_ingreso: roundNumber(entity.createdAt, 3),
      minuto_salida: null,
      duracion_minutos: roundNumber(Number(state.simTime || 0) - Number(entity.createdAt || 0), 3),
      espera_minutos: roundNumber(entity.totalWaitMinutes, 3),
      eventos_demora: Number(entity.delayEvents || 0),
      atencion_minutos: roundNumber(entity.totalServiceMinutes, 3),
      espera_actual_minutos: entity.queueEnteredAt === null || entity.queueEnteredAt === undefined
        ? 0
        : roundNumber(Number(state.simTime || 0) - Number(entity.queueEnteredAt || 0), 3),
      estado_emocional: entity.mood === "angry" ? "Molesto" : "Tranquilo",
      minuto_molestia: roundNumber(entity.becameAngryAt, 3),
      evaluacion_abandono_realizada: Boolean(entity.abandonEvaluated),
      nodo_cola_actual: queueNodeId,
      estado_final: entity.finalStatus || "En proceso",
      resultado_credito: entity.creditOutcome || null,
      motivo_credito: entity.creditReason || null,
      ingreso_mensual_usd: profile ? roundMoney(profile.monthlyIncome) : null,
      deuda_mensual_usd: profile ? roundMoney(profile.currentMonthlyDebt) : null,
      antiguedad_laboral_meses: profile ? Number(profile.employmentMonths || 0) : null,
      documentos_completos: profile ? Boolean(profile.documentsComplete) : null,
      historial_crediticio: profile ? profile.creditHistory : null,
      tasa_interes_anual_pct: roundNumber(entity.creditAnnualInterestRate, 6),
      plazo_meses: entity.creditTermMonths ? Number(entity.creditTermMonths) : null,
      cuota_mensual_usd: roundMoney(entity.creditMonthlyPayment),
      interes_total_usd: roundMoney(entity.creditTotalInterest),
      total_pagar_usd: roundMoney(entity.creditTotalPayable),
      resultado_operacion: entity.operationOutcome || null,
      motivo_operacion: entity.operationReason || null,
      direccion_transferencia: entity.transferDirection || null,
      efecto_capital_usd: entity.capitalEffect === null || entity.capitalEffect === undefined
        ? null
        : safeMovementEffect(entity.capitalMovement, entity.transactionType, entity.capitalEffect, entity.creditType),
      capital_antes_usd: roundMoney(entity.capitalBefore),
      capital_despues_usd: roundMoney(entity.capitalAfter),
      movimiento_capital: entity.capitalMovement || null,
      movimiento_financiero_registrado: Boolean(entity.financialPosted),
      areas_visitadas: Array.isArray(entity.route) ? entity.route.length : 0,
      ruta: Array.isArray(entity.route) ? entity.route.join(" → ") : "",
      ruta_json: normalizeSerializable(entity.route || [], `personas.${entity.label}.route`),
      pasos: normalizeSerializable(entity.steps || [], `personas.${entity.label}.steps`),
      linea_tiempo: normalizeSerializable(entity.timeline || [], `personas.${entity.label}.timeline`),
      datos_adicionales: normalizeSerializable(entity, `personas.${entity.label}`)
    };
  }

  function personRowFromRecord(record) {
    return {
      persona_codigo: String(record.personId),
      tipo_usuario: record.personType || "Ciudadano",
      con_cita: record.hasAppointment === "Sí" || record.hasAppointment === true,
      prioridad: record.priorityLevel || "Regular",
      canal: record.serviceChannel || "Presencial",
      revision_cumplimiento: record.complianceFlag === "Sí" || record.complianceFlag === true,
      satisfaccion_1_5: roundNumber(record.satisfactionScore, 3),
      categoria_nps: record.satisfactionCategory || null,
      tramite_tipo: record.transactionType || null,
      tramite_nombre: record.transactionLabel || null,
      tramite_codigo: record.transactionCode || null,
      monto_usd: safeTransactionAmount(record.transactionType, record.amountUsd, record.creditType),
      tipo_credito: record.creditType || null,
      tipo_credito_nombre: record.creditTypeLabel || null,
      fecha_ingreso: record.startDateTime || simulationIsoDateTime(record.startTime),
      fecha_salida: record.endDateTime || simulationIsoDateTime(record.endTime),
      minuto_ingreso: roundNumber(record.startTime, 3),
      minuto_salida: roundNumber(record.endTime, 3),
      duracion_minutos: roundNumber(record.durationMinutes, 3),
      espera_minutos: roundNumber(record.waitingMinutes, 3),
      eventos_demora: Number(record.delayEvents || 0),
      atencion_minutos: roundNumber(record.serviceMinutes, 3),
      espera_actual_minutos: 0,
      estado_emocional: record.mood || "Tranquilo",
      minuto_molestia: roundNumber(record.becameAngryAt, 3),
      evaluacion_abandono_realizada: true,
      nodo_cola_actual: null,
      estado_final: record.finalStatus || "Completado",
      resultado_credito: record.creditOutcome || null,
      motivo_credito: record.creditReason || null,
      ingreso_mensual_usd: roundMoney(record.monthlyIncome),
      deuda_mensual_usd: roundMoney(record.currentMonthlyDebt),
      antiguedad_laboral_meses: record.employmentMonths === "" || record.employmentMonths === null ? null : Number(record.employmentMonths || 0),
      documentos_completos: record.documentsComplete === "" || record.documentsComplete === null
        ? null
        : (record.documentsComplete === "Sí" || record.documentsComplete === true),
      historial_crediticio: record.creditHistory || null,
      tasa_interes_anual_pct: roundNumber(record.creditAnnualInterestRate, 6),
      plazo_meses: record.creditTermMonths ? Number(record.creditTermMonths) : null,
      cuota_mensual_usd: roundMoney(record.creditMonthlyPayment),
      interes_total_usd: roundMoney(record.creditTotalInterest),
      total_pagar_usd: roundMoney(record.creditTotalPayable),
      resultado_operacion: record.operationOutcome || null,
      motivo_operacion: record.operationReason || null,
      direccion_transferencia: record.transferDirection || null,
      efecto_capital_usd: record.capitalEffect === null || record.capitalEffect === undefined || record.capitalEffect === ""
        ? null
        : safeMovementEffect(record.capitalMovement, record.transactionType, record.capitalEffect, record.creditType),
      capital_antes_usd: roundMoney(record.capitalBefore),
      capital_despues_usd: roundMoney(record.capitalAfter),
      movimiento_capital: record.capitalMovement || null,
      movimiento_financiero_registrado: Boolean(record.capitalMovement || record.capitalEffect),
      areas_visitadas: Number(record.areasVisited || 0),
      ruta: record.route || "",
      ruta_json: typeof record.route === "string" ? record.route.split(" → ").filter(Boolean) : normalizeSerializable(record.route || []),
      pasos: normalizeSerializable(record.steps || [], `records.${record.personId}.steps`),
      linea_tiempo: normalizeSerializable(record.timeline || [], `records.${record.personId}.timeline`),
      datos_adicionales: normalizeSerializable(record, `records.${record.personId}`)
    };
  }

  function collectPersonRows(entityMap, recordMap, commits, full) {
    const combined = new Map();
    entityMap.forEach((entity, code) => combined.set(code, personRowFromEntity(entity)));
    recordMap.forEach((record, code) => combined.set(code, personRowFromRecord(record)));
    const rows = [];
    combined.forEach((row, code) => {
      const signature = stableSignature(row);
      if (full || personSignatures.get(code) !== signature) {
        rows.push(row);
        commits.persons.push([code, signature]);
      }
    });
    return rows;
  }

  function collectSteps(entityMap, recordMap, commits, full) {
    const collections = new Map();
    entityMap.forEach((entity, code) => collections.set(code, entity.steps || []));
    recordMap.forEach((record, code) => collections.set(code, record.steps || []));
    const rows = [];
    collections.forEach((steps, code) => {
      if (!Array.isArray(steps)) return;
      const previousCount = full ? 0 : Number(stepCounts.get(code) || 0);
      steps.slice(previousCount).forEach((step, offset) => {
        const index = previousCount + offset;
        const minute = roundNumber(step.minute ?? step.time, 3);
        rows.push({
          persona_codigo: code,
          indice_paso: index,
          nodo_id: step.nodeId || step.node_id || null,
          nodo_nombre: step.nodeName || step.name || step.area || null,
          tipo_nodo: step.nodeType || step.type || null,
          departamento: step.department || null,
          procedimiento: step.procedure || null,
          etapa: step.stage || step.event || null,
          fecha_hora: step.dateTime || (minute !== null ? simulationIsoDateTime(minute) : null),
          fecha: step.date || (minute !== null ? formatDateIso(simulationDate(minute)) : null),
          hora: step.clock || (minute !== null ? simulationClock(minute) : null),
          minuto_simulado: minute,
          datos_adicionales: normalizeSerializable(step, `pasos.${code}[${index}]`)
        });
      });
      if (steps.length > previousCount) commits.steps.push([code, steps.length]);
    });
    return rows;
  }

  function collectTimelineEvents(entityMap, recordMap, commits, full) {
    const timelines = new Map();
    entityMap.forEach((entity, code) => timelines.set(code, { source: entity, timeline: entity.timeline || [] }));
    recordMap.forEach((record, code) => timelines.set(code, { source: record, timeline: record.timeline || [] }));
    const rows = [];
    let remaining = full ? Number.POSITIVE_INFINITY : maxEventsPerSync;

    timelines.forEach(({ source, timeline }, code) => {
      if (!Array.isArray(timeline) || remaining <= 0) return;
      const previousCount = full ? 0 : Number(timelineCounts.get(code) || 0);
      const available = timeline.slice(previousCount, Number.isFinite(remaining) ? previousCount + remaining : undefined);
      available.forEach((event, offset) => {
        const index = previousCount + offset;
        const minute = roundNumber(event.minute, 3);
        rows.push({
          persona_codigo: code,
          indice_evento: index,
          evento: event.event || event.stage || "Evento",
          fecha_hora: event.dateTime || (minute !== null ? simulationIsoDateTime(minute) : null),
          fecha: event.date || (minute !== null ? formatDateIso(simulationDate(minute)) : null),
          hora: event.clock || (minute !== null ? simulationClock(minute) : null),
          minuto_simulado: minute,
          area: event.area || event.nodeName || null,
          departamento: event.department || null,
          procedimiento: event.procedure || null,
          etapa: event.stage || null,
          motivo: event.reason || null,
          monto_usd: safeTransactionAmount(source?.transactionType || inferTransactionType(event.movement), event.amount, source?.creditType || event.creditType),
          espera_minutos: roundNumber(event.waitedMinutes, 3),
          objetivo_espera_minutos: roundNumber(event.targetMinutes, 3),
          movimiento: event.movement || null,
          efecto_capital_usd: event.effect === undefined || event.effect === null
            ? null
            : safeMovementEffect(event.movement, source?.transactionType, event.effect, source?.creditType || event.creditType),
          capital_antes_usd: roundMoney(event.capitalBefore),
          capital_despues_usd: roundMoney(event.capitalAfter),
          tipo_credito: event.creditType || null,
          tipo_credito_nombre: event.creditTypeLabel || null,
          resultado_credito: event.outcome || null,
          tasa_anual_pct: roundNumber(event.annualInterestRate, 6),
          plazo_meses: event.termMonths === undefined ? null : Number(event.termMonths),
          cuota_mensual_usd: roundMoney(event.monthlyPayment),
          interes_total_usd: roundMoney(event.totalInterest),
          total_pagar_usd: roundMoney(event.totalPayable),
          ratio_endeudamiento_pct: roundNumber(event.projectedDebtRatio, 6),
          datos_adicionales: normalizeSerializable(event, `eventos.${code}[${index}]`)
        });
      });
      if (available.length > 0) {
        commits.timelines.push([code, previousCount + available.length]);
        if (Number.isFinite(remaining)) remaining -= available.length;
      }
    });
    return rows;
  }

  function movementKey(row) {
    return [
      row.persona_codigo || "",
      row.minuto_simulado ?? "",
      row.tipo_movimiento || "",
      row.monto_usd ?? "",
      row.efecto_capital_usd ?? "",
      row.capital_antes_usd ?? "",
      row.capital_despues_usd ?? ""
    ].join("|");
  }

  function collectMovementRows(entityMap, recordMap, commits, full) {
    const candidates = new Map();
    state.capitalLedger.forEach((entry, index) => {
      const safeEffect = safeMovementEffect(entry.movementType || entry.movement, entry.transactionType, entry.amount, entry.creditType);
      const row = {
        persona_codigo: entry.personId || null,
        tramite_tipo: entry.transactionType || null,
        tipo_movimiento: entry.movementType || entry.movement || null,
        descripcion: entry.description || null,
        monto_usd: Math.abs(safeEffect),
        efecto_capital_usd: safeEffect,
        capital_antes_usd: roundMoney(entry.before),
        capital_despues_usd: roundMoney(entry.after),
        fecha_hora: entry.dateTime || (Number.isFinite(Number(entry.minute)) ? simulationIsoDateTime(Number(entry.minute)) : null),
        fecha: entry.date || null,
        hora: entry.clock || null,
        minuto_simulado: roundNumber(entry.minute, 3),
        datos_adicionales: normalizeSerializable(entry, `state.capitalLedger[${index}]`)
      };
      row.clave_evento = movementKey(row);
      candidates.set(row.clave_evento, row);
    });

    const timelineSources = new Map();
    entityMap.forEach((entity, code) => timelineSources.set(code, { source: entity, timeline: entity.timeline || [] }));
    recordMap.forEach((record, code) => timelineSources.set(code, { source: record, timeline: record.timeline || [] }));
    timelineSources.forEach(({ source, timeline }, code) => {
      if (!Array.isArray(timeline)) return;
      timeline.forEach((event, index) => {
        if (event.event !== "Movimiento de capital") return;
        const effect = safeMovementEffect(event.movement || source.capitalMovement, source.transactionType, event.effect, source.creditType || event.creditType);
        const row = {
          persona_codigo: code,
          tramite_tipo: source.transactionType || null,
          tipo_movimiento: event.movement || source.capitalMovement || null,
          descripcion: source.operationReason || source.transactionLabel || event.movement || null,
          monto_usd: Math.abs(effect),
          efecto_capital_usd: effect,
          capital_antes_usd: roundMoney(event.capitalBefore),
          capital_despues_usd: roundMoney(event.capitalAfter),
          fecha_hora: event.dateTime || (Number.isFinite(Number(event.minute)) ? simulationIsoDateTime(Number(event.minute)) : null),
          fecha: event.date || null,
          hora: event.clock || null,
          minuto_simulado: roundNumber(event.minute, 3),
          datos_adicionales: normalizeSerializable(event, `movimientos.${code}[${index}]`)
        };
        row.clave_evento = movementKey(row);
        if (!candidates.has(row.clave_evento)) candidates.set(row.clave_evento, row);
      });
    });

    const rows = [];
    candidates.forEach((row, key) => {
      if (full || !movementKeys.has(key)) {
        rows.push(row);
        commits.movements.push(key);
      }
    });
    return rows;
  }

  function findCreditNode() {
    return state.nodes.find(node => node.type === "process" && node.props?.processRole === "credit_evaluation")
      || state.nodes.find(node => node.type === "process" && /cr[eé]dito/i.test(node.name || ""))
      || null;
  }

  function collectCreditRows(entityMap, recordMap, commits, full) {
    const sources = new Map();
    entityMap.forEach((entity, code) => { if (entity.creditOutcome) sources.set(code, entity); });
    recordMap.forEach((record, code) => { if (record.creditOutcome) sources.set(code, record); });
    const creditNode = findCreditNode();
    const props = creditNode?.props || {};
    const rows = [];

    sources.forEach((source, code) => {
      const timeline = source.timeline || [];
      const evaluation = [...timeline].reverse().find(event => event.event === "Evaluación de crédito") || {};
      const profile = source.creditProfile || null;
      const row = {
        persona_codigo: code,
        fecha_hora: evaluation.dateTime || (Number.isFinite(Number(evaluation.minute)) ? simulationIsoDateTime(Number(evaluation.minute)) : null),
        minuto_simulado: roundNumber(evaluation.minute, 3),
        tipo_credito: source.creditType || null,
        tipo_credito_nombre: source.creditTypeLabel || null,
        resultado: source.creditOutcome || null,
        motivo: source.creditReason || null,
        monto_solicitado_usd: safeTransactionAmount("credit", source.amountUsd, source.creditType || evaluation.creditType),
        ingreso_mensual_usd: profile ? roundMoney(profile.monthlyIncome) : roundMoney(source.monthlyIncome),
        deuda_mensual_usd: profile ? roundMoney(profile.currentMonthlyDebt) : roundMoney(source.currentMonthlyDebt),
        antiguedad_laboral_meses: profile ? Number(profile.employmentMonths || 0) : (source.employmentMonths === "" ? null : Number(source.employmentMonths || 0)),
        documentos_completos: profile ? Boolean(profile.documentsComplete) : (source.documentsComplete === "" ? null : (source.documentsComplete === "Sí" || source.documentsComplete === true)),
        historial_crediticio: profile ? profile.creditHistory : (source.creditHistory || null),
        ingreso_minimo_requerido_usd: roundMoney(props.creditMinIncome ?? 600),
        ratio_deuda_maximo_pct: roundNumber(props.creditMaxDebtRatio ?? 40, 6),
        antiguedad_minima_meses: Number(props.creditMinEmploymentMonths ?? 6),
        multiplo_ingreso_maximo: roundNumber(props.creditMaxIncomeMultiple ?? 12, 6),
        tasa_aprobacion_configurada_pct: roundNumber(props.creditApprovalRate ?? 75, 6),
        tasa_anual_pct: roundNumber(source.creditAnnualInterestRate || evaluation.annualInterestRate, 6),
        plazo_meses: Number(source.creditTermMonths || evaluation.termMonths || 0) || null,
        cuota_mensual_usd: roundMoney(source.creditMonthlyPayment || evaluation.monthlyPayment),
        interes_total_usd: roundMoney(source.creditTotalInterest || evaluation.totalInterest),
        total_pagar_usd: roundMoney(source.creditTotalPayable || evaluation.totalPayable),
        ratio_endeudamiento_proyectado_pct: roundNumber(evaluation.projectedDebtRatio, 6),
        datos_adicionales: normalizeSerializable({ source, evaluation, configuracion_credito: props }, `creditos.${code}`)
      };
      const signature = stableSignature(row);
      if (full || creditSignatures.get(code) !== signature) {
        rows.push(row);
        commits.credits.push([code, signature]);
      }
    });
    return rows;
  }

  function collectLoanRows(commits, full) {
    const rows = [];
    state.loanAccounts.forEach((loan, index) => {
      const row = {
        credito_codigo: String(loan.id || `credito-${index}`),
        persona_codigo: loan.personId || null,
        tipo_credito: loan.creditType || null,
        principal_inicial_usd: roundMoney(loan.principal),
        principal_pendiente_usd: roundMoney(loan.outstandingPrincipal),
        tasa_anual_pct: roundNumber(loan.annualRate, 6),
        plazo_meses: Number(loan.termMonths || 0),
        cuota_mensual_usd: roundMoney(loan.monthlyPayment),
        fecha_inicio: loan.startDate || null,
        cuotas_procesadas: Number(loan.installmentsProcessed || 0),
        monto_vencido_usd: roundMoney(loan.overdueAmount),
        estado: loan.status || "current",
        datos_adicionales: normalizeSerializable(loan, `state.loanAccounts[${index}]`)
      };
      const signature = stableSignature(row);
      if (full || loanSignatures.get(row.credito_codigo) !== signature) {
        rows.push(row);
        commits.loans.push([row.credito_codigo, signature]);
      }
    });
    return rows;
  }

  function buildNodeRows() {
    return state.nodes.map((node, index) => ({
      nodo_id: String(node.id),
      tipo: node.type || "",
      nombre: node.name || null,
      posicion_x: roundNumber(node.x, 6),
      posicion_y: roundNumber(node.y, 6),
      departamento: node.props?.department || null,
      procedimiento: node.props?.procedure || null,
      observaciones: node.props?.observations || null,
      rol_proceso: node.props?.processRole || null,
      capacidad: node.props?.capacity === undefined ? null : Number(node.props.capacity),
      intervalo_minutos: roundNumber(node.props?.interval, 6),
      tiempo_servicio_minutos: roundNumber(node.props?.serviceTime, 6),
      objetivo_espera_minutos: roundNumber(node.props?.targetWaitMinutes, 6),
      molestia_desde_minutos: roundNumber(node.props?.angerAfterMinutes, 6),
      abandono_desde_minutos: roundNumber(node.props?.abandonAfterMinutes, 6),
      probabilidad_abandono_pct: roundNumber(node.props?.abandonProbability, 6),
      costo_personal_hora_usd: roundMoney(node.props?.staffCostPerHour),
      efectivo_minimo_usd: roundMoney(node.props?.cashMinimum),
      efectivo_maximo_usd: roundMoney(node.props?.cashMaximum),
      monto_fijo_usd: roundMoney(node.props?.amountFixed),
      monto_minimo_usd: roundMoney(node.props?.amountMin),
      monto_maximo_usd: roundMoney(node.props?.amountMax),
      rangos_automaticos_usd: normalizeSerializable(node.props?.automaticAmountRanges || {}, `state.nodes[${index}].props.automaticAmountRanges`),
      rangos_credito_usd: normalizeSerializable(node.props?.creditAmountRanges || {}, `state.nodes[${index}].props.creditAmountRanges`),
      tasas_credito_pct: normalizeSerializable(node.props?.creditRates || {}, `state.nodes[${index}].props.creditRates`),
      plazos_credito_meses: normalizeSerializable(node.props?.creditTerms || {}, `state.nodes[${index}].props.creditTerms`),
      propiedades: normalizeSerializable(node.props || {}, `state.nodes[${index}].props`),
      datos_adicionales: normalizeSerializable(node, `state.nodes[${index}]`)
    }));
  }

  function buildEdgeRows() {
    return state.edges.map((edge, index) => ({
      conexion_id: String(edge.id),
      nodo_origen_id: String(edge.from),
      nodo_destino_id: String(edge.to),
      peso: roundNumber(edge.weight, 6),
      tipos_transaccion: normalizeSerializable(edge.transactionTypes || [], `state.edges[${index}].transactionTypes`),
      resultados_credito: normalizeSerializable(edge.creditOutcomes || edge.creditResults || [], `state.edges[${index}].creditOutcomes`),
      datos_adicionales: normalizeSerializable(edge, `state.edges[${index}]`)
    }));
  }

  function buildScenarioRows() {
    return state.scenarioSnapshots.map((snapshot, index) => ({
      escenario_codigo: String(snapshot.id || snapshot.scenarioId || `escenario-${index}`),
      nombre: snapshot.name || snapshot.nombre || `Escenario ${index + 1}`,
      guardado_en: snapshot.savedAt || snapshot.createdAt || new Date().toISOString(),
      personal: Number(snapshot.staff || snapshot.personal || 0),
      tramites_completados: Number(snapshot.completed || snapshot.tramitesCompletados || 0),
      espera_promedio_minutos: roundNumber(snapshot.avgWait ?? snapshot.averageWait ?? snapshot.esperaPromedio, 3),
      usuarios_molestos: Number(snapshot.angry ?? snapshot.angryUsers ?? snapshot.usuariosMolestos ?? 0),
      abandonos: Number(snapshot.abandoned ?? snapshot.abandonedUsers ?? snapshot.abandonos ?? 0),
      capital_usd: roundMoney(snapshot.capital || snapshot.currentCapital),
      patrimonio_estimado_usd: roundMoney(snapshot.equity || snapshot.estimatedEquity),
      creditos_aprobados: Number(snapshot.creditApproved || snapshot.creditosAprobados || 0),
      creditos_rechazados: Number(snapshot.creditRejected || snapshot.creditosRechazados || 0),
      cartera_vencida_usd: roundMoney(snapshot.overduePortfolio || snapshot.carteraVencida),
      costo_personal_usd: roundMoney(snapshot.staffCost || snapshot.costoPersonal),
      satisfaccion: roundNumber(snapshot.satisfaction || snapshot.satisfaccion, 3),
      nps: roundNumber(snapshot.nps, 3),
      utilizacion_pct: roundNumber(snapshot.utilization || snapshot.utilizacion, 3),
      datos_adicionales: normalizeSerializable(snapshot, `state.scenarioSnapshots[${index}]`)
    }));
  }

  function calculateCurrentIndicators() {
    const completedRecords = state.records.length;
    const averageWait = completedRecords
      ? state.records.reduce((sum, record) => sum + Number(record.waitingMinutes || 0), 0) / completedRecords
      : 0;
    const averageSatisfaction = state.metrics.satisfactionSamples
      ? Number(state.metrics.satisfactionTotal || 0) / Number(state.metrics.satisfactionSamples || 1)
      : 0;
    const nps = state.metrics.satisfactionSamples
      ? ((Number(state.metrics.promoters || 0) - Number(state.metrics.detractors || 0)) / Number(state.metrics.satisfactionSamples || 1)) * 100
      : 0;
    const utilization = state.metrics.staffCapacityMinutes
      ? (Number(state.metrics.staffBusyMinutes || 0) / Number(state.metrics.staffCapacityMinutes || 1)) * 100
      : 0;
    const wip = Math.max(0, Number(state.metrics.created || 0) - Number(state.metrics.completed || 0) - Number(state.metrics.rejected || 0));
    const equity = typeof estimatedEquity === "function"
      ? estimatedEquity()
      : Number(state.scenario.currentCapital || 0) + Number(state.financial.loanPortfolio || 0) - Number(state.financial.clientDeposits || 0) - Number(state.financial.investmentLiability || 0);
    return { averageWait, averageSatisfaction, nps, utilization, wip, equity };
  }

  function buildMetricRows(snapshotKey) {
    const current = calculateCurrentIndicators();
    return [{
      clave_snapshot: snapshotKey,
      fecha_hora_real: new Date().toISOString(),
      fecha_hora_simulada: simulationIsoDateTime(),
      fecha_simulada: formatDateIso(simulationDate()),
      hora_simulada: simulationClock(),
      minuto_simulado: roundNumber(state.simTime, 3),
      estado: runStatus,
      personas_ingresadas: Number(state.metrics.created || 0),
      personas_finalizadas: Number(state.metrics.completed || 0),
      personas_rechazadas: Number(state.metrics.rejected || 0),
      personas_en_sistema: current.wip,
      cola_maxima: Number(state.metrics.maxQueue || 0),
      espera_promedio_minutos: roundNumber(current.averageWait, 3),
      personal_total: typeof totalStaff === "function" ? Number(totalStaff() || 0) : 0,
      usuarios_molestos: Number(state.metrics.angryUsers || 0),
      abandonos: Number(state.metrics.abandonedUsers || 0),
      eventos_demora: Number(state.metrics.delayedEvents || 0),
      capital_disponible_usd: moneyOrZero(state.scenario.currentCapital),
      cambio_neto_capital_usd: moneyOrZero(Number(state.scenario.currentCapital || 0) - Number(state.scenario.initialCapital || 0)),
      depositos_usd: moneyOrZero(state.metrics.depositTotal),
      retiros_usd: moneyOrZero(state.metrics.withdrawalTotal),
      pagos_usd: moneyOrZero(state.metrics.paymentTotal),
      transferencias_entrantes_usd: moneyOrZero(state.metrics.transferInTotal),
      transferencias_salientes_usd: moneyOrZero(state.metrics.transferOutTotal),
      creditos_desembolsados_usd: moneyOrZero(state.metrics.creditDisbursed),
      intereses_por_cobrar_usd: moneyOrZero(state.metrics.interestReceivable),
      pagos_credito_recibidos_usd: moneyOrZero(state.metrics.loanPaymentsReceived),
      efectivo_caja_usd: moneyOrZero(state.financial.cashDesk),
      efectivo_boveda_usd: moneyOrZero(state.financial.vaultCash),
      depositos_clientes_usd: moneyOrZero(state.financial.clientDeposits),
      pasivo_inversiones_usd: moneyOrZero(state.financial.investmentLiability),
      cartera_creditos_usd: moneyOrZero(state.financial.loanPortfolio),
      cartera_vencida_usd: moneyOrZero(state.financial.overduePortfolio),
      recuperado_usd: moneyOrZero(state.financial.recoveredTotal),
      ingresos_comisiones_usd: moneyOrZero(state.financial.commissionIncome),
      intereses_cobrados_usd: moneyOrZero(state.financial.interestCollected),
      costo_personal_usd: moneyOrZero(state.financial.staffCost),
      costo_operativo_usd: moneyOrZero(state.financial.operatingCost),
      patrimonio_estimado_usd: moneyOrZero(current.equity),
      creditos_aprobados: Number(state.metrics.creditApproved || 0),
      creditos_rechazados: Number(state.metrics.creditRejected || 0),
      creditos_morosos: Number(state.metrics.delinquentLoans || 0),
      satisfaccion_acumulada: roundNumber(state.metrics.satisfactionTotal, 3) || 0,
      muestras_satisfaccion: Number(state.metrics.satisfactionSamples || 0),
      satisfaccion_promedio: roundNumber(current.averageSatisfaction, 3) || 0,
      promotores: Number(state.metrics.promoters || 0),
      detractores: Number(state.metrics.detractors || 0),
      nps: roundNumber(current.nps, 3) || 0,
      minutos_personal_ocupado: roundNumber(state.metrics.staffBusyMinutes, 3) || 0,
      minutos_capacidad_personal: roundNumber(state.metrics.staffCapacityMinutes, 3) || 0,
      utilizacion_personal_pct: roundNumber(current.utilization, 3) || 0,
      alertas_cumplimiento: Number(state.metrics.complianceFlags || 0),
      operaciones_bloqueadas: Number(state.metrics.blockedOperations || 0),
      historial_metricas: normalizeSerializable(state.metrics.history || [], "state.metrics.history"),
      metricas: normalizeSerializable(state.metrics, "state.metrics"),
      estado_financiero: normalizeSerializable(state.financial, "state.financial"),
      runtime_nodos: serializeNodeRuntime(),
      datos_adicionales: {
        evento_operativo: normalizeSerializable(state.operationalEvent, "state.operationalEvent"),
        eventos_programados: serializeScheduledEvents()
      }
    }];
  }

  function buildSimulationRow(entityMap, completeState) {
    const current = calculateCurrentIndicators();
    const runtimeState = buildRuntimeState();
    return {
      id: runId,
      version_esquema: 3,
      nombre: String(state.scenario.businessName || "Entidad financiera"),
      estado: runStatus,
      moneda: "USD",
      fecha_inicio_real: startedAt || new Date().toISOString(),
      fecha_fin_real: endedAt,
      fecha_hora_simulada: simulationIsoDateTime(),
      fecha_simulada: formatDateIso(simulationDate()),
      hora_simulada: simulationClock(),
      minuto_simulado: numberOrZero(state.simTime, 3),
      fecha_inicio_escenario: state.scenario.startDate || null,
      fecha_fin_escenario: state.scenario.endDate || null,
      unidad_simulacion: state.scenario.simulationUnit || null,
      longitud_simulacion: Number(state.scenario.simulationLength || 0),
      dias_operacion: normalizeSerializable(state.scenario.operatingDays || [], "state.scenario.operatingDays"),
      hora_apertura: state.scenario.openingTime || null,
      hora_cierre: state.scenario.closingTime || null,
      capital_inicial_usd: moneyOrZero(state.scenario.initialCapital),
      capital_actual_usd: moneyOrZero(state.scenario.currentCapital),
      reserva_minima_usd: moneyOrZero(state.scenario.minimumReserve),
      efectivo_boveda_inicial_usd: moneyOrZero(state.scenario.initialVaultCash),
      efectivo_caja_inicial_usd: moneyOrZero(state.scenario.initialCashDesk),
      umbral_aml_usd: moneyOrZero(state.scenario.amlThreshold),
      tasa_comision_pct: numberOrZero(state.scenario.commissionRate, 6),
      tasa_mora_mensual_pct: numberOrZero(state.scenario.monthlyDefaultRate, 6),
      tasa_recuperacion_pct: numberOrZero(state.scenario.recoveryRate, 6),
      tasa_citas_pct: numberOrZero(state.scenario.appointmentRate, 6),
      tasa_prioridad_pct: numberOrZero(state.scenario.priorityRate, 6),
      participacion_digital_pct: numberOrZero(state.scenario.digitalShare, 6),
      tasa_falla_diaria_pct: numberOrZero(state.scenario.dailyOutageRate, 6),
      duracion_falla_minutos: Number(state.scenario.outageMinutes || 0),
      efectivo_caja_usd: moneyOrZero(state.financial.cashDesk),
      efectivo_boveda_usd: moneyOrZero(state.financial.vaultCash),
      depositos_clientes_usd: moneyOrZero(state.financial.clientDeposits),
      pasivo_inversiones_usd: moneyOrZero(state.financial.investmentLiability),
      cartera_creditos_usd: moneyOrZero(state.financial.loanPortfolio),
      cartera_vencida_usd: moneyOrZero(state.financial.overduePortfolio),
      recuperado_total_usd: moneyOrZero(state.financial.recoveredTotal),
      ingresos_comisiones_usd: moneyOrZero(state.financial.commissionIncome),
      intereses_cobrados_usd: moneyOrZero(state.financial.interestCollected),
      costo_personal_usd: moneyOrZero(state.financial.staffCost),
      costo_operativo_usd: moneyOrZero(state.financial.operatingCost),
      patrimonio_estimado_usd: moneyOrZero(current.equity),
      escenario: normalizeSerializable(state.scenario, "state.scenario"),
      estado_financiero: normalizeSerializable(state.financial, "state.financial"),
      metricas: normalizeSerializable(state.metrics, "state.metrics"),
      modelo: normalizeSerializable({ nodes: state.nodes, edges: state.edges }, "state.model"),
      definiciones: buildDefinitions(),
      escenarios_guardados: normalizeSerializable(state.scenarioSnapshots, "state.scenarioSnapshots"),
      evento_operativo: normalizeSerializable(state.operationalEvent, "state.operationalEvent"),
      entidades_activas: Array.from(entityMap.values()).map(entity => normalizeSerializable(entity, `activeEntities.${entity.label}`)),
      estado_runtime: runtimeState,
      estado_completo: completeState || {}
    };
  }

  function buildSnapshot(snapshotKey, completeState) {
    return {
      estado: runStatus,
      fecha_hora_real: new Date().toISOString(),
      fecha_hora_simulada: simulationIsoDateTime(),
      fecha_simulada: formatDateIso(simulationDate()),
      hora_simulada: simulationClock(),
      minuto_simulado: numberOrZero(state.simTime, 3),
      moneda: "USD",
      capital_actual_usd: moneyOrZero(state.scenario.currentCapital),
      efectivo_caja_usd: moneyOrZero(state.financial.cashDesk),
      efectivo_boveda_usd: moneyOrZero(state.financial.vaultCash),
      cartera_creditos_usd: moneyOrZero(state.financial.loanPortfolio),
      cartera_vencida_usd: moneyOrZero(state.financial.overduePortfolio),
      escenario: normalizeSerializable(state.scenario, "state.scenario"),
      estado_financiero: normalizeSerializable(state.financial, "state.financial"),
      metricas: normalizeSerializable(state.metrics, "state.metrics"),
      modelo: normalizeSerializable({ nodes: state.nodes, edges: state.edges }, "state.model"),
      runtime: buildRuntimeState(),
      estado_completo: completeState || {},
      clave_snapshot: snapshotKey
    };
  }

  function flattenVariables(value, path = "state", category = "state", rows = []) {
    if (value === null || value === undefined) {
      rows.push({
        ruta_variable: path,
        categoria: category,
        tipo_dato: "null",
        es_usd: false,
        moneda: null,
        valor_usd: null,
        valor_numerico: null,
        valor_booleano: null,
        valor_texto: null,
        valor_json: null
      });
      return rows;
    }

    if (Array.isArray(value)) {
      if (value.length === 0) {
        rows.push({ ruta_variable: path, categoria: category, tipo_dato: "array", es_usd: false, moneda: null, valor_usd: null, valor_numerico: null, valor_booleano: null, valor_texto: null, valor_json: [] });
      } else {
        value.forEach((item, index) => flattenVariables(item, `${path}[${index}]`, category, rows));
      }
      return rows;
    }

    if (typeof value === "object") {
      const keys = Object.keys(value);
      if (keys.length === 0) {
        rows.push({ ruta_variable: path, categoria: category, tipo_dato: "object", es_usd: false, moneda: null, valor_usd: null, valor_numerico: null, valor_booleano: null, valor_texto: null, valor_json: {} });
      } else {
        keys.forEach(key => flattenVariables(value[key], `${path}.${key}`, category, rows));
      }
      return rows;
    }

    const usd = typeof value === "number" && isUsdPath(path);
    const type = typeof value;
    rows.push({
      ruta_variable: path,
      categoria: category,
      tipo_dato: usd ? "usd" : type,
      es_usd: usd,
      moneda: usd ? "USD" : null,
      valor_usd: usd ? moneyOrZero(value) : null,
      valor_numerico: type === "number" && !usd ? Number(value) : null,
      valor_booleano: type === "boolean" ? value : null,
      valor_texto: type === "string" ? value : null,
      valor_json: null
    });
    return rows;
  }

  function buildVariableRows(completeState, commits, full) {
    const rows = [];
    const roots = {
      state: completeState,
      definitions: buildDefinitions(),
      synchronization: {
        runId,
        startedAt,
        endedAt,
        runStatus,
        active,
        projectUrl,
        schemaVersion: 2
      }
    };
    Object.entries(roots).forEach(([category, value]) => flattenVariables(value, category, category, rows));

    const changed = [];
    rows.forEach(row => {
      const signature = stableSignature(row);
      if (full || variableSignatures.get(row.ruta_variable) !== signature) {
        changed.push(row);
        commits.variables.push([row.ruta_variable, signature]);
      }
    });
    return { all: rows, changed };
  }

  function buildPayload(forceFull = false) {
    const now = Date.now();
    const complete = Boolean(forceFull || now - lastFullSnapshotAt >= fullSnapshotIntervalMs);
    if (complete) lastFullSnapshotAt = now;

    const commits = {
      persons: [],
      steps: [],
      timelines: [],
      movements: [],
      credits: [],
      loans: [],
      variables: [],
      metricKey: ""
    };

    const entityMap = collectEntityMap();
    const recordMap = collectRecordMap();
    const completeState = complete ? buildCompleteState() : {};
    const syncId = createUuid();
    const snapshotKey = `${runId}:${Date.now()}:${numberOrZero(state.simTime, 3)}:${syncId.slice(0, 8)}`;
    const variableRows = buildVariableRows(complete ? completeState : normalizeSerializable({
      scenario: state.scenario,
      financial: state.financial,
      metrics: state.metrics,
      runtime: buildRuntimeState()
    }, "state"), commits, complete);

    const metricSignature = [
      runStatus,
      numberOrZero(state.simTime, 3),
      Number(state.metrics.created || 0),
      Number(state.metrics.completed || 0),
      Number(state.metrics.rejected || 0),
      moneyOrZero(state.scenario.currentCapital),
      state.capitalLedger.length,
      state.records.length
    ].join("|");
    const metricRows = complete || metricSignature !== lastMetricKey ? buildMetricRows(snapshotKey) : [];
    if (metricRows.length) commits.metricKey = metricSignature;

    const payload = {
      sync_id: syncId,
      clave_snapshot: snapshotKey,
      es_snapshot_completo: complete,
      reemplazar_variables: complete,
      reemplazar_modelo: complete,
      reemplazar_runtime: complete,
      reemplazar_escenarios: complete,
      simulacion: buildSimulationRow(entityMap, completeState),
      snapshot: buildSnapshot(snapshotKey, completeState),
      variables: complete ? variableRows.all : variableRows.changed,
      variables_cambiadas: variableRows.changed,
      nodos: buildNodeRows(),
      conexiones: buildEdgeRows(),
      runtime_nodos: serializeNodeRuntime(),
      personas: collectPersonRows(entityMap, recordMap, commits, complete),
      pasos: collectSteps(entityMap, recordMap, commits, complete),
      eventos: collectTimelineEvents(entityMap, recordMap, commits, complete),
      movimientos: collectMovementRows(entityMap, recordMap, commits, complete),
      creditos: collectCreditRows(entityMap, recordMap, commits, complete),
      cartera: collectLoanRows(commits, complete),
      escenarios: buildScenarioRows(),
      metricas: metricRows
    };

    return { payload, commits, complete };
  }

  function applyCommits(commits) {
    commits.persons.forEach(([key, signature]) => personSignatures.set(key, signature));
    commits.steps.forEach(([key, count]) => stepCounts.set(key, count));
    commits.timelines.forEach(([key, count]) => timelineCounts.set(key, count));
    commits.movements.forEach(key => movementKeys.add(key));
    commits.credits.forEach(([key, signature]) => creditSignatures.set(key, signature));
    commits.loans.forEach(([key, signature]) => loanSignatures.set(key, signature));
    commits.variables.forEach(([key, signature]) => variableSignatures.set(key, signature));
    if (commits.metricKey) lastMetricKey = commits.metricKey;
  }

  function clearRunCaches() {
    personSignatures.clear();
    stepCounts.clear();
    timelineCounts.clear();
    movementKeys.clear();
    creditSignatures.clear();
    loanSignatures.clear();
    variableSignatures.clear();
    lastMetricKey = "";
    lastFullSnapshotAt = 0;
  }

  function openOutboxDb() {
    return new Promise((resolve, reject) => {
      if (!window.indexedDB) {
        reject(new Error("IndexedDB no está disponible"));
        return;
      }
      const request = window.indexedDB.open(OUTBOX_DB, 1);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(OUTBOX_STORE)) {
          db.createObjectStore(OUTBOX_STORE, { keyPath: "queue_key" });
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error || new Error("No se pudo abrir IndexedDB"));
    });
  }

  async function idbPut(record) {
    const db = await openOutboxDb();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(OUTBOX_STORE, "readwrite");
      transaction.objectStore(OUTBOX_STORE).put(record);
      transaction.oncomplete = () => { db.close(); resolve(true); };
      transaction.onerror = () => { db.close(); reject(transaction.error); };
      transaction.onabort = () => { db.close(); reject(transaction.error); };
    });
  }

  async function idbGet(queueKey) {
    const db = await openOutboxDb();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(OUTBOX_STORE, "readonly");
      const request = transaction.objectStore(OUTBOX_STORE).get(queueKey);
      request.onsuccess = () => { db.close(); resolve(request.result || null); };
      request.onerror = () => { db.close(); reject(request.error); };
    });
  }

  async function idbDelete(queueKey) {
    const db = await openOutboxDb();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(OUTBOX_STORE, "readwrite");
      transaction.objectStore(OUTBOX_STORE).delete(queueKey);
      transaction.oncomplete = () => { db.close(); resolve(true); };
      transaction.onerror = () => { db.close(); reject(transaction.error); };
    });
  }

  function fallbackKey(queueKey) {
    return `${OUTBOX_FALLBACK_PREFIX}${queueKey}`;
  }

  async function saveOutbox(record) {
    try {
      await idbPut(record);
      try { window.localStorage.removeItem(fallbackKey(record.queue_key)); } catch (error) {}
      return true;
    } catch (error) {
      try {
        window.localStorage.setItem(fallbackKey(record.queue_key), JSON.stringify(record));
        return true;
      } catch (storageError) {
        console.error("No se pudo conservar el lote pendiente:", storageError);
        return false;
      }
    }
  }

  async function loadOutbox(queueKey) {
    try {
      const record = await idbGet(queueKey);
      if (record) return record;
    } catch (error) {}
    try {
      const raw = window.localStorage.getItem(fallbackKey(queueKey));
      return raw ? JSON.parse(raw) : null;
    } catch (error) {
      return null;
    }
  }

  async function deleteOutbox(queueKey) {
    try { await idbDelete(queueKey); } catch (error) {}
    try { window.localStorage.removeItem(fallbackKey(queueKey)); } catch (error) {}
  }

  async function sendPayload(payload, keepalive = false) {
    const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    const timeoutId = controller ? window.setTimeout(() => controller.abort(), requestTimeoutMs) : null;
    try {
      const response = await fetch(rpcUrl, {
        method: "POST",
        headers: {
          apikey: publishableKey,
          "Content-Type": "application/json",
          Accept: "application/json"
        },
        body: JSON.stringify({ p_payload: payload }),
        keepalive,
        signal: controller?.signal
      });
      const responseText = await response.text();
      if (!response.ok) {
        let detail = responseText;
        try {
          const parsed = JSON.parse(responseText);
          detail = parsed.message || parsed.details || parsed.hint || responseText;
        } catch (error) {}
        throw new Error(`Supabase ${response.status}: ${detail}`);
      }
      return responseText ? JSON.parse(responseText) : { ok: true };
    } finally {
      if (timeoutId) window.clearTimeout(timeoutId);
    }
  }

  async function flushOutbox(queueKey = runId, keepalive = false) {
    if (!queueKey || !isConfigured()) return false;
    const record = await loadOutbox(queueKey);
    if (!record) return true;
    const response = await sendPayload(record.payload, keepalive);
    await deleteOutbox(queueKey);
    applyCommits(record.commits || { persons: [], steps: [], timelines: [], movements: [], credits: [], loans: [], variables: [], metricKey: "" });
    return Boolean(response?.ok !== false);
  }

  async function syncNow(forceFull = false, keepalive = false) {
    if (!runId || !isConfigured()) return false;
    if (syncing) {
      syncRequested = true;
      return false;
    }

    syncing = true;
    setConnectionStatus("Supabase: preparando datos", "syncing");
    try {
      const { payload, commits } = buildPayload(forceFull);
      const queueRecord = {
        queue_key: runId,
        sync_id: payload.sync_id,
        created_at: new Date().toISOString(),
        payload,
        commits
      };
      const persisted = await saveOutbox(queueRecord);
      if (!persisted) throw new Error("No fue posible conservar localmente el lote antes de enviarlo");

      setConnectionStatus("Supabase: sincronizando", "syncing");
      await flushOutbox(runId, keepalive);
      setConnectionStatus("Supabase: todo guardado", "connected");
      return true;
    } catch (error) {
      console.error("Error al sincronizar con Supabase:", error);
      setConnectionStatus(navigator.onLine ? "Supabase: pendiente" : "Supabase: sin conexión", "error");
      if (!keepalive && typeof showToast === "function") {
        showToast("Los datos quedaron en la cola local y se reenviarán automáticamente.", "warning");
      }
      return false;
    } finally {
      syncing = false;
      if (syncRequested) {
        syncRequested = false;
        window.setTimeout(() => { void syncNow(false, false); }, 200);
      }
    }
  }

  function requestSync(forceFull = false) {
    if (!runId || !isConfigured()) return;
    syncRequested = true;
    window.clearTimeout(requestSync.debounceId);
    requestSync.debounceId = window.setTimeout(() => {
      syncRequested = false;
      void syncNow(forceFull, false);
    }, forceFull ? 0 : 250);
  }

  function beginRun() {
    if (!isConfigured()) {
      setConnectionStatus("Supabase: sin configurar", "error");
      if (typeof showToast === "function") {
        showToast("Configure la Publishable key en supabase-config.js para guardar la simulación.", "warning");
      }
      return "";
    }
    runId = createUuid();
    startedAt = new Date().toISOString();
    endedAt = null;
    runStatus = "ejecutando";
    active = true;
    clearRunCaches();
    persistSession();
    requestSync(true);
    return runId;
  }

  function markStatus(status) {
    if (!runId) return;
    runStatus = String(status || runStatus);
    active = !["finalizada", "detenida", "reiniciada"].includes(runStatus);
    persistSession();
    requestSync(true);
  }

  function finishRun(status = "finalizada") {
    if (!runId) return Promise.resolve(false);
    runStatus = String(status || "finalizada");
    endedAt = new Date().toISOString();
    active = false;
    persistSession();
    return syncNow(true, true);
  }

  function initialize() {
    if (initialized) return;
    initialized = true;
    restoreSession();

    if (isConfigured()) {
      setConnectionStatus(runId ? "Supabase: sesión recuperada" : "Supabase: listo", "connected");
    } else {
      setConnectionStatus("Supabase: sin configurar", "error");
    }

    timer = window.setInterval(() => {
      if (runId && (active || state.running || state.paused)) void syncNow(false, false);
    }, syncIntervalMs);

    window.addEventListener("online", () => {
      if (runId) void syncNow(true, false);
    });

    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "hidden" && runId) void syncNow(true, true);
    });

    window.addEventListener("pagehide", () => {
      if (runId) void syncNow(true, true);
    });

    if (runId && isConfigured()) {
      window.setTimeout(() => { void syncNow(true, false); }, 600);
    }
  }

  return {
    initialize,
    beginRun,
    markStatus,
    finishRun,
    requestSync,
    syncNow,
    isConfigured,
    get runId() { return runId; },
    get active() { return active; }
  };
})();
