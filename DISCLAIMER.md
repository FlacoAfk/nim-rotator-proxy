# Disclaimer / Descargo de responsabilidad

## English

**Independent project.** nim-rotator-proxy is an independent, community-made HTTP
proxy. It is **not affiliated with, endorsed by, or sponsored by NVIDIA
Corporation**. "NVIDIA", "NIM" and related names and marks are trademarks of
NVIDIA Corporation. This project redistributes **no models, weights, or NVIDIA
content** — it only forwards HTTP requests that *you* are already authorized to
make to NVIDIA's endpoints.

**No warranty.** The software is provided "AS IS" under the MIT License,
without warranty of any kind. The authors and contributors are **not liable**
for any damage, loss, account suspension, key revocation, or legal issue
arising from its use.

**You are solely responsible for your API keys and your usage.** Every request
this proxy makes goes to NVIDIA under *your* (or your users') API key, and is
governed by NVIDIA's own agreements — not by this project:

- [NVIDIA API Trial Terms of Service](https://assets.ngc.nvidia.com/products/api-catalog/legal/NVIDIA%20API%20Trial%20Terms%20of%20Service.pdf)
  — the catalog endpoints are a **trial service for evaluation/prototyping, not
  production** (§1.2, §1.4). §4.2 restricts making the API Service or generated
  content available to others. NVIDIA may change, discontinue or deprecate any
  part of the service at any time (§8) and may revoke keys.
- [NVIDIA Developer Terms](https://developer.nvidia.com/legal/terms) — API
  credentials are **for your use only** and are **your sole responsibility**;
  circumventing technical limits or using the service to dodge fees/quotas is
  prohibited.
- [NVIDIA Privacy Policy](https://www.nvidia.com/en-us/about-nvidia/privacy-policy/)
  — your requests and inputs are processed by NVIDIA under its privacy policy
  (usage monitoring is disclosed in Trial TOS §3.3).

**Key sharing guidance.** The key-pool feature exists so the *operator* can
rotate among their **own** keys on their **own** machines. Sharing pooled keys
with third parties may conflict with NVIDIA's terms (Trial TOS §4.2). For
multiple people, use **BYOK**: every user sends their *own* `nvapi-` key, keeps
their own quota, and stays individually responsible to NVIDIA. The operator
chooses how to configure the proxy and is responsible for that choice.

**Generated content.** Models served through NVIDIA's catalog may produce
inaccurate, biased or harmful output. Per Trial TOS §2.5, the **user** assumes
the risk of any model response. The proxy authors do not control, filter, or
take responsibility for model outputs.

**Privacy of the operator.** The proxy logs request metadata locally
(`data/proxy.log`, hash-prefixed key ids, token counts). Run it on
infrastructure you control and review local data-protection obligations
yourself.

---

## Español

**Proyecto independiente.** nim-rotator-proxy es un proxy HTTP hecho por la
comunidad. **No está afiliado a, avalado por ni patrocinado por NVIDIA
Corporation**. "NVIDIA", "NIM" y sus marcas son propiedad de NVIDIA Corporation.
Este proyecto **no redistribuye modelos, pesos ni contenido de NVIDIA** — solo
reenvía peticiones HTTP que *vos ya estás autorizado* a hacer a los endpoints
de NVIDIA.

**Sin garantía.** El software se provee "AS IS" bajo la licencia MIT, sin
garantía de ningún tipo. Los autores y contribuyentes **no se hacen
responsables** por daños, pérdidas, suspensión de cuentas, revocación de keys
o problemas legales derivados de su uso.

**El usuario es el único responsable de sus API keys y de su uso.** Cada
petición que hace este proxy va a NVIDIA bajo *tu* (o la de tus usuarios) API
key, y se rige por los acuerdos propios de NVIDIA — no por este proyecto:

- [NVIDIA API Trial Terms of Service](https://assets.ngc.nvidia.com/products/api-catalog/legal/NVIDIA%20API%20Trial%20Terms%20of%20Service.pdf)
  — los endpoints del catálogo son un **servicio de prueba para evaluación y
  prototipado, no producción** (§1.2, §1.4). El §4.2 restringe poner el
  servicio o su contenido a disposición de otros. NVIDIA puede cambiar,
  discontinuar o deprecar cualquier parte del servicio en cualquier momento
  (§8) y revocar keys.
- [NVIDIA Developer Terms](https://developer.nvidia.com/legal/terms) — las
  credenciales de API son **para tu uso exclusivo** y son **tu única
  responsabilidad**; eludir límites técnicos o usar el servicio para evadir
  cuotas o fees está prohibido.
- [NVIDIA Privacy Policy](https://www.nvidia.com/en-us/about-nvidia/privacy-policy/)
  — tus requests e inputs son procesados por NVIDIA bajo su política de
  privacidad (el monitoreo de uso está divulgado en el TOS de prueba §3.3).

**Sobre compartir keys.** El pool de keys existe para que el *operador* rote
entre sus **propias** keys en sus **propias** máquinas. Compartir keys del pool
con terceros puede conflictuar con los términos de NVIDIA (TOS de prueba §4.2).
Para varias personas, usá **BYOK**: cada usuario manda *su propia* key
`nvapi-`, mantiene su propia cuota y responde individualmente ante NVIDIA. El
operador decide cómo configurar el proxy y es responsable de esa decisión.

**Contenido generado.** Los modelos del catálogo de NVIDIA pueden producir
salidas inexactas, sesgadas o dañinas. Según el TOS de prueba §2.5, el
**usuario** asume el riesgo de toda respuesta del modelo. Los autores del proxy
no controlan, filtran ni se responsabilizan por las salidas de los modelos.

**Privacidad del operador.** El proxy registra metadatos localmente
(`data/proxy.log`, ids de key con hash, conteo de tokens). Corrélo en
infraestructura propia y revisá tus obligaciones locales de protección de
datos.
