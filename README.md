# TurnoJusto · AP-U1-Munoz-Melanie

**Melanie Michelle Muñoz Vera** · Universidad Técnica Estatal de Quevedo · Aplicaciones Distribuidas (ISR-701), grupo 2.

Tres ventanillas comparten una cola FIFO. Cada solicitud abre una conexión TCP con el coordinador, que guarda en memoria el último número emitido, los turnos pendientes y el último latido de cada ventanilla. Al reiniciar el coordinador comienza una nueva numeración.

[Informe de escenarios (PDF)](docs/informe.pdf) · [Entrega para el SGA (PDF)](docs/entrega_SGA.pdf) · [Capturas](docs/img/) · [Registros](logs/)

## Compilación y arranque en Windows

Desde la raíz del repositorio, con JDK 21 o posterior y PowerShell, compilar una vez:

```powershell
New-Item -ItemType Directory -Force out | Out-Null
javac --release 21 -encoding UTF-8 -d out src/TurnoJusto/TurnoJusto.java
```

En **cada terminal**, desde esa misma raíz, cargar la configuración. El prototipo lee propiedades de la JVM. Este bloque convierte las entradas de `config.properties` en argumentos `-D`. El puerto se comparte entre los clientes y el servidor. El host indica a los clientes dónde conectar.

```powershell
$cfg = ConvertFrom-StringData (Get-Content -Raw config.properties)
$opciones = @($cfg.GetEnumerator() | ForEach-Object { "-D$($_.Key)=$($_.Value)" })
$carpeta = 'logs/E1'
function tj { java @opciones "-Dturno.logs=$carpeta" -cp out TurnoJusto @args }
```

1. En la primera terminal ejecutar `tj coordinador`.
2. Abrir tres terminales más y ejecutar respectivamente `tj ventanilla 1`, `tj ventanilla 2` y `tj ventanilla 3`. Cada ventanilla acepta `turno`, `llamar`, `estado` y `salir`. Envía un latido cada dos segundos.
3. Para una solicitud aislada usar, por ejemplo, `tj solicitud 2 LLAMAR`. Se admiten los identificadores 1 a 3 y las operaciones TURNO, LLAMAR, ESTADO y LATIDO. Esta modalidad termina tras la respuesta.
4. Cerrar cada ventanilla con `salir` y detener el coordinador con Ctrl+C. Para repetir las pruebas y conservar las evidencias publicadas, copiar el repositorio a otra carpeta o usar una raíz nueva, como `logs-repeticion`, en todos los procesos. Los registros individuales se amplían en cada ejecución.

## Secuencia de pruebas

Usar un coordinador recién iniciado por escenario y por modo de carga. Antes de arrancar los procesos, asignar a `$carpeta` la subcarpeta del escenario en todas las terminales. Para documentar la sesión ejecutar `whoami`, `hostname` y `Get-Date` en PowerShell.

| Escenario | Operaciones |
|---|---|
| E0 · Sin red | Al inicio del laboratorio, mostrar `Get-NetAdapter \| Format-Table Name,Status` junto con la identidad, el equipo y la hora. Falta la captura original. |
| E1 · Cola compartida | Pedir turno en V1, V2, V3 y otra vez en V1. Llamar desde V2 y V3. Consultar estado en las tres sin operaciones intermedias. |
| E2 · Rechazos | Con la cola vacía, ejecutar `tj solicitud 1 LLAMAR`. Ejecutar `1..11 \| ForEach-Object { tj solicitud 1 TURNO }`. Para el token inválido usar el comando de abajo en otra terminal. |
| E3 · Carga | En dos terminales usar `$carpeta='logs/E3/normal'`, arrancar `tj coordinador` y luego `tj carga`. Detener el coordinador. Cambiar ambas carpetas a `logs/E3/inseguro` y repetir con `tj coordinador --inseguro`. Cada prueba lanza 30 solicitudes. |
| E4 · V3 caída | Iniciar V3 y dejar que envíe latidos. Cerrar V3, esperar el aviso de caída y volver a ejecutar `tj ventanilla 3`. |
| E5 · Coordinador caído | Iniciar las tres ventanillas, comprobar su estado, detener solo el coordinador y escribir `turno` en V1. |
| E6 · Orden total | Tras detener los procesos de E1 a E5, cambiar a `$carpeta='logs'` y ejecutar `tj orden`. Usar la raíz nueva si se trabaja con una repetición. |

Para E2, el token incorrecto se aplica solo al cliente (usar el puerto configurado):

```powershell
java "-Dturno.host=$($cfg['turno.host'])" "-Dturno.puerto=$($cfg['turno.puerto'])" "-Dturno.logs=$carpeta" "-Dturno.token=incorrecto" -cp out TurnoJusto solicitud 3 TURNO
```

## Mensajes y registros

El protocolo intercambia una línea UTF-8 de solicitud y otra de respuesta por conexión. El separador es `|`; el salto de línea termina el mensaje.

```text
tipo|token|ventanilla|lamport
resultado|detalle|numero|emitidos|cupo|lamport
```

Por ejemplo, con cupo 10 y una cola nueva, una petición `TURNO|practica-token|1|1` puede recibir `ASIGNADO|turno_creado|1|1|10|3`. LLAMAR extrae el primero de la cola. ESTADO devuelve la cola y el estado de cada ventanilla en el detalle. LATIDO devuelve OK. Una respuesta RECHAZADA incluye el motivo y número −1. Si falla la conexión, la ventanilla genera un ERROR local.

Cada proceso escribe `lamport;idProceso;evento;detalle`. El coordinador usa el identificador 0. La orden `orden` reúne los archivos `.log` bajo E1 a E5, incluso en sus subcarpetas, excluye los archivos llamados `orden_total.log` y genera uno nuevo en la raíz seleccionada. Ordena por reloj de Lamport, identificador de proceso y línea original, y conserva la ruta de origen. Los relojes se reinician al arrancar un proceso. Para interpretar causalidad hay que comparar eventos de la misma ejecución.

## Evidencia y documentos

Las cuatro capturas originales del laboratorio se conservan en `docs/img/img*.png`. Los archivos `E1-local.png` a `E6-local.png` muestran en una consola de Windows la identidad, el equipo, la hora de consulta y las salidas guardadas de la reproducción posterior. Los registros originales de carga están en `logs/prueba-normal` y `logs/prueba-insegura`. Los de la reproducción están en `logs/E1` a `logs/E6`, con el resultado combinado en `logs/orden_total.log`.

Para compilar con **pdfLaTeX**, desde la raíz:

```powershell
Push-Location docs
pdflatex -halt-on-error informe.tex
pdflatex -halt-on-error informe.tex
pdflatex -halt-on-error entrega_SGA.tex
pdflatex -halt-on-error entrega_SGA.tex
Pop-Location
```

Los archivos principales son [informe.tex](docs/informe.tex) y [entrega_SGA.tex](docs/entrega_SGA.tex). Compilar desde `docs` conserva las rutas relativas de las capturas.

## Código reutilizado

No se reutilizó código.
