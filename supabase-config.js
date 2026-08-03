/* Configuración pública optimizada del simulador financiero.
   Coloque únicamente la Publishable key de Supabase.
   No use Secret key, service_role ni la contraseña PostgreSQL. */
window.SUPABASE_CONFIG = Object.freeze({
  url: "https://balvytaccwngchtyvllh.supabase.co",
  publishableKey: "sb_publishable_2e0Pz4lUSRFR-SnABW736w_dvlDNbcn",

  // Guardado automático por microlotes.
  syncIntervalMs: 2000,
  eventDebounceMs: 500,

  // El modelo/configuración completa se envía solo cada 5 minutos y al finalizar.
  fullSnapshotIntervalMs: 300000,

  // Las métricas históricas se conservan cada 30 segundos simulados/reales.
  metricIntervalMs: 30000,

  maxEventsPerSync: 1500,
  requestTimeoutMs: 30000
});
