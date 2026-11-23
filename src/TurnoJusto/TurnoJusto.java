import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class TurnoJusto {

    private static final String HOST = System.getProperty("turno.host", "127.0.0.1");
    private static final int PUERTO = Integer.getInteger("turno.puerto", 5050);
    private static final String TOKEN = System.getProperty("turno.token", "practica-token");
    private static final int CUPO = Integer.getInteger("turno.cupo", 10);
    private static final Path CARPETA_LOGS = Path.of("logs");
    private static final Map<Path, Object> CERROJOS_LOG = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            mostrarAyuda();
            return;
        }

        switch (args[0].toLowerCase(Locale.ROOT)) {
            case "coordinador" -> {
                boolean inseguro = args.length > 1 && args[1].equalsIgnoreCase("--inseguro");
                new Coordinador(inseguro).iniciar();
            }
            case "ventanilla" -> {
                if (args.length < 2) {
                    throw new IllegalArgumentException("Indique la ventanilla: 1, 2 o 3");
                }
                int id = Integer.parseInt(args[1]);
                if (id < 1 || id > 3) {
                    throw new IllegalArgumentException("La ventanilla debe ser 1, 2 o 3");
                }
                new Ventanilla(id, true).iniciarConsola();
            }
            case "carga" -> ejecutarPruebaCarga();
            case "orden" -> generarOrdenTotal();
            default -> mostrarAyuda();
        }
    }

    private static void mostrarAyuda() {
        System.out.println("""
                TURNO JUSTO 

                Compile:
                  javac TurnoJusto.java

                Ejecute cada comando en una terminal diferente:
                  java TurnoJusto coordinador
                  java TurnoJusto ventanilla 1
                  java TurnoJusto ventanilla 2
                  java TurnoJusto ventanilla 3

                Prueba concurrente (coordinador ya iniciado):
                  java TurnoJusto carga

                Comparacion con condicion de carrera:
                  java TurnoJusto coordinador --inseguro
                  java TurnoJusto carga

                Unir logs por Lamport e id de proceso:
                  java TurnoJusto orden

                Ordenes de una ventanilla: turno, llamar, estado, salir
                """);
    }

    // ---------------------------------------------------------------------
    // COORDINADOR
    // ---------------------------------------------------------------------

    private static final class Coordinador {
        private final boolean inseguro;
        private final Object cerrojoEstado = new Object();
        private final AtomicInteger lamport = new AtomicInteger();
        private final Queue<Integer> turnosEnEspera = new ArrayDeque<>();
        private final Map<Integer, Long> ultimoLatido = new ConcurrentHashMap<>();
        private final Map<Integer, String> estadoAnterior = new ConcurrentHashMap<>();
        private int ultimoTurnoEmitido = 0;

        private Coordinador(boolean inseguro) {
            this.inseguro = inseguro;
        }

        private void iniciar() throws IOException {
            Files.createDirectories(CARPETA_LOGS);
            ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread hilo = new Thread(r, "monitor-latidos");
                hilo.setDaemon(true);
                return hilo;
            });
            monitor.scheduleAtFixedRate(this::revisarLatidos, 1, 1, TimeUnit.SECONDS);

            ExecutorService trabajadores = Executors.newCachedThreadPool();
            System.out.printf("Coordinador en %s:%d | cupo=%d | modo=%s%n",
                    HOST, PUERTO, CUPO, inseguro ? "INSEGURO" : "NORMAL");
            registrar(0, siguienteLamport(), "INICIO",
                    "puerto=" + PUERTO + ",cupo=" + CUPO + ",inseguro=" + inseguro);

            try (ServerSocket servidor = new ServerSocket(PUERTO)) {
                while (true) {
                    Socket socket = servidor.accept();
                    trabajadores.submit(() -> atender(socket));
                }
            } finally {
                trabajadores.shutdownNow();
                monitor.shutdownNow();
            }
        }

        private void atender(Socket socket) {
            try (socket;
                 BufferedReader entrada = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                 BufferedWriter salida = new BufferedWriter(
                         new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {

                String linea = entrada.readLine(); // El salto de linea delimita cada mensaje.
                Solicitud solicitud = Solicitud.desdeLinea(linea);
                int recibido = recibirLamport(solicitud.lamport());
                registrar(0, recibido, "RECIBE",
                        solicitud.tipo() + " desde V" + solicitud.ventanilla());

                Respuesta respuesta;
                if (!TOKEN.equals(solicitud.token())) {
                    respuesta = respuesta("RECHAZADA", "token_invalido", -1);
                    registrar(0, siguienteLamport(), "RECHAZO",
                            "token_invalido,V" + solicitud.ventanilla());
                } else {
                    respuesta = procesar(solicitud);
                }

                int selloEnvio = siguienteLamport();
                respuesta = respuesta.conLamport(selloEnvio);
                registrar(0, selloEnvio, "ENVIA",
                        respuesta.resultado() + " a V" + solicitud.ventanilla());
                salida.write(respuesta.aLinea());
                salida.newLine();
                salida.flush();
            } catch (Exception error) {
                int sello = siguienteLamport();
                registrar(0, sello, "ERROR", limpiar(error.getMessage()));
                System.err.println("Error atendiendo solicitud: " + error.getMessage());
            }
        }

        private Respuesta procesar(Solicitud solicitud) throws InterruptedException {
            return switch (solicitud.tipo()) {
                case "TURNO" -> inseguro ? asignarTurnoInseguro() : asignarTurnoSeguro();
                case "LLAMAR" -> llamarSiguiente();
                case "ESTADO" -> consultarEstado();
                case "LATIDO" -> recibirLatido(solicitud.ventanilla());
                default -> respuesta("RECHAZADA", "tipo_desconocido", -1);
            };
        }

        private Respuesta asignarTurnoSeguro() {
            synchronized (cerrojoEstado) {
                if (ultimoTurnoEmitido >= CUPO) {
                    return respuesta("RECHAZADA", "cupo_agotado", -1);
                }
                int numero = ++ultimoTurnoEmitido;
                turnosEnEspera.add(numero);
                return respuesta("ASIGNADO", "turno_creado", numero);
            }
        }


        private Respuesta asignarTurnoInseguro() throws InterruptedException {
            int candidato = ultimoTurnoEmitido + 1;
            if (candidato > CUPO) {
                return respuesta("RECHAZADA", "cupo_agotado", -1);
            }
            Thread.sleep(50);
            ultimoTurnoEmitido = candidato;
            synchronized (cerrojoEstado) {
                turnosEnEspera.add(candidato);
            }
            return respuesta("ASIGNADO", "turno_creado_sin_exclusion", candidato);
        }

        private Respuesta llamarSiguiente() {
            synchronized (cerrojoEstado) {
                Integer numero = turnosEnEspera.poll();
                if (numero == null) {
                    return respuesta("RECHAZADA", "sin_turnos_en_espera", -1);
                }
                return respuesta("LLAMADO", "atender_turno", numero);
            }
        }

        private Respuesta consultarEstado() {
            synchronized (cerrojoEstado) {
                String detalle = "espera=" + turnosEnEspera
                        + ",ventanillas=" + describirVentanillas();
                return respuesta("ESTADO", detalle, -1);
            }
        }

        private Respuesta recibirLatido(int idVentanilla) {
            long ahora = System.currentTimeMillis();
            ultimoLatido.put(idVentanilla, ahora);
            String anterior = estadoAnterior.put(idVentanilla, "ACTIVA");
            if ("CAIDA".equals(anterior)) {
                int sello = siguienteLamport();
                registrar(0, sello, "VENTANILLA_ACTIVA", "V" + idVentanilla);
                System.out.println("V" + idVentanilla + " volvio a estar ACTIVA");
            }
            return respuesta("OK", "latido_recibido", -1);
        }

        private void revisarLatidos() {
            long ahora = System.currentTimeMillis();
            for (int id = 1; id <= 3; id++) {
                Long ultimo = ultimoLatido.get(id);
                if (ultimo == null) {
                    continue;
                }
                String actual = ahora - ultimo > 6_000 ? "CAIDA" : "ACTIVA";
                String anterior = estadoAnterior.put(id, actual);
                if ("CAIDA".equals(actual) && !"CAIDA".equals(anterior)) {
                    int sello = siguienteLamport();
                    registrar(0, sello, "VENTANILLA_CAIDA", "V" + id);
                    System.out.println("V" + id + " marcada como CAIDA");
                }
            }
        }

        private String describirVentanillas() {
            List<String> estados = new ArrayList<>();
            long ahora = System.currentTimeMillis();
            for (int id = 1; id <= 3; id++) {
                Long ultimo = ultimoLatido.get(id);
                String estado = ultimo == null ? "SIN_LATIDO"
                        : (ahora - ultimo > 6_000 ? "CAIDA" : "ACTIVA");
                estados.add("V" + id + ":" + estado);
            }
            return String.join(",", estados);
        }

        private Respuesta respuesta(String resultado, String detalle, int numero) {
            int emitidos;
            synchronized (cerrojoEstado) {
                emitidos = ultimoTurnoEmitido;
            }
            return new Respuesta(resultado, detalle, numero, emitidos, CUPO, 0);
        }

        private int siguienteLamport() {
            return lamport.incrementAndGet();
        }

        private int recibirLamport(int remoto) {
            return lamport.updateAndGet(local -> Math.max(local, remoto) + 1);
        }
    }

    // VENTANILLA

    private static final class Ventanilla implements AutoCloseable {
        private final int id;
        private final AtomicInteger lamport = new AtomicInteger();
        private final ScheduledExecutorService latidos;

        private Ventanilla(int id, boolean activarLatidos) {
            this.id = id;
            if (activarLatidos) {
                latidos = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread hilo = new Thread(r, "latidos-v" + id);
                    hilo.setDaemon(true);
                    return hilo;
                });
                latidos.scheduleAtFixedRate(() -> enviarSilencioso("LATIDO"),
                        0, 2, TimeUnit.SECONDS);
            } else {
                latidos = null;
            }
        }

        private void iniciarConsola() throws IOException {
            Files.createDirectories(CARPETA_LOGS);
            System.out.printf("Ventanilla V%d. Ordenes: turno, llamar, estado, salir%n", id);
            try (BufferedReader teclado = new BufferedReader(
                    new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                while (true) {
                    System.out.print("V" + id + "> ");
                    String orden = teclado.readLine();
                    if (orden == null || orden.equalsIgnoreCase("salir")) {
                        break;
                    }
                    String tipo = switch (orden.toLowerCase(Locale.ROOT)) {
                        case "turno" -> "TURNO";
                        case "llamar" -> "LLAMAR";
                        case "estado" -> "ESTADO";
                        default -> null;
                    };
                    if (tipo == null) {
                        System.out.println("Orden no reconocida.");
                        continue;
                    }
                    Respuesta respuesta = enviar(tipo);
                    System.out.println(respuesta.paraHumano());
                }
            } finally {
                close();
            }
        }

        private Respuesta enviar(String tipo) {
            int selloEnvio = lamport.incrementAndGet();
            registrar(id, selloEnvio, "ENVIA", tipo + " al coordinador");
            Solicitud solicitud = new Solicitud(tipo, TOKEN, id, selloEnvio);

            try (Socket socket = new Socket(HOST, PUERTO);
                 BufferedWriter salida = new BufferedWriter(
                         new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                 BufferedReader entrada = new BufferedReader(
                         new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {

                socket.setSoTimeout(4_000);
                salida.write(solicitud.aLinea());
                salida.newLine();
                salida.flush();

                Respuesta respuesta = Respuesta.desdeLinea(entrada.readLine());
                int selloRecepcion = lamport.updateAndGet(
                        local -> Math.max(local, respuesta.lamport()) + 1);
                registrar(id, selloRecepcion, "RECIBE",
                        respuesta.resultado() + " del coordinador");
                return respuesta;
            } catch (IOException error) {
                int selloError = lamport.incrementAndGet();
                registrar(id, selloError, "COORDINADOR_NO_DISPONIBLE", limpiar(error.getMessage()));
                return new Respuesta("ERROR", "coordinador_no_disponible", -1, -1, CUPO,
                        selloError);
            }
        }

        private void enviarSilencioso(String tipo) {
            Respuesta respuesta = enviar(tipo);
            if ("ERROR".equals(respuesta.resultado())) {
                System.err.println("V" + id + ": coordinador no disponible");
            }
        }

        @Override
        public void close() {
            if (latidos != null) {
                latidos.shutdownNow();
            }
        }
    }

    // PRUEBA DE CARGA


    private static void ejecutarPruebaCarga() throws Exception {
        Files.createDirectories(CARPETA_LOGS);
        ExecutorService ejecutor = Executors.newFixedThreadPool(30);
        CountDownLatch salidaSimultanea = new CountDownLatch(1);
        List<Future<Respuesta>> futuros = new ArrayList<>();
        Ventanilla[] clientes = {
                new Ventanilla(1, false),
                new Ventanilla(2, false),
                new Ventanilla(3, false)
        };

        for (Ventanilla cliente : clientes) {
            for (int i = 0; i < 10; i++) {
                futuros.add(ejecutor.submit(() -> {
                    salidaSimultanea.await();
                    return cliente.enviar("TURNO");
                }));
            }
        }

        long inicio = System.nanoTime();
        salidaSimultanea.countDown();
        List<Respuesta> respuestas = new ArrayList<>();
        for (Future<Respuesta> futuro : futuros) {
            respuestas.add(futuro.get());
        }
        long milisegundos = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - inicio);
        ejecutor.shutdown();

        long asignadas = respuestas.stream()
                .filter(r -> r.resultado().equals("ASIGNADO")).count();
        long rechazadas = respuestas.stream()
                .filter(r -> r.resultado().equals("RECHAZADA")).count();
        Set<Integer> distintos = new HashSet<>();
        respuestas.stream().filter(r -> r.numero() > 0)
                .forEach(r -> distintos.add(r.numero()));
        Respuesta estado = clientes[0].enviar("ESTADO");

        System.out.println("\nRESULTADO DE PRUEBA CARGA");
        System.out.println("Solicitudes: 30");
        System.out.println("Asignadas: " + asignadas);
        System.out.println("Rechazadas: " + rechazadas);
        System.out.println("Numeros distintos: " + distintos.size() + " -> " + distintos);
        System.out.println("Coordinador dice emitidos: " + estado.emitidos());
        System.out.println("Tiempo: " + milisegundos + " ms");
    }

    // ---------------------------------------------------------------------
    // ORDEN TOTAL DE LOS LOGS
    // ---------------------------------------------------------------------

    private static void generarOrdenTotal() throws IOException {
        Files.createDirectories(CARPETA_LOGS);
        List<EventoLog> eventos = new ArrayList<>();
        try (var archivos = Files.list(CARPETA_LOGS)) {
            for (Path archivo : archivos
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .filter(p -> !p.getFileName().toString().equals("orden_total.log"))
                    .toList()) {
                for (String linea : Files.readAllLines(archivo, StandardCharsets.UTF_8)) {
                    EventoLog evento = EventoLog.desdeLinea(linea);
                    if (evento != null) {
                        eventos.add(evento);
                    }
                }
            }
        }

        eventos.sort(Comparator.comparingInt(EventoLog::lamport)
                .thenComparingInt(EventoLog::idProceso));
        Path destino = CARPETA_LOGS.resolve("orden_total.log");
        Files.write(destino, eventos.stream().map(EventoLog::lineaOriginal).toList(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("Orden total generado: " + destino.toAbsolutePath());
        System.out.println("Eventos combinados: " + eventos.size());
    }

    // ---------------------------------------------------------------------
    // MENSAJES Y UTILIDADES
    // ---------------------------------------------------------------------

    private record Solicitud(String tipo, String token, int ventanilla, int lamport) {
        private String aLinea() {
            return tipo + "|" + token + "|" + ventanilla + "|" + lamport;
        }

        private static Solicitud desdeLinea(String linea) {
            if (linea == null) {
                throw new IllegalArgumentException("Solicitud vacia");
            }
            String[] partes = linea.split("\\|", -1);
            if (partes.length != 4) {
                throw new IllegalArgumentException("Solicitud mal formada");
            }
            return new Solicitud(partes[0], partes[1], Integer.parseInt(partes[2]),
                    Integer.parseInt(partes[3]));
        }
    }

    private record Respuesta(String resultado, String detalle, int numero,
                             int emitidos, int cupo, int lamport) {
        private String aLinea() {
            return String.join("|", resultado, limpiar(detalle), String.valueOf(numero),
                    String.valueOf(emitidos), String.valueOf(cupo), String.valueOf(lamport));
        }

        private static Respuesta desdeLinea(String linea) {
            if (linea == null) {
                throw new IllegalArgumentException("Respuesta vacia");
            }
            String[] partes = linea.split("\\|", -1);
            if (partes.length != 6) {
                throw new IllegalArgumentException("Respuesta mal formada");
            }
            return new Respuesta(partes[0], partes[1], Integer.parseInt(partes[2]),
                    Integer.parseInt(partes[3]), Integer.parseInt(partes[4]),
                    Integer.parseInt(partes[5]));
        }

        private Respuesta conLamport(int nuevoLamport) {
            return new Respuesta(resultado, detalle, numero, emitidos, cupo, nuevoLamport);
        }

        private String paraHumano() {
            return "resultado=" + resultado + ", numero=" + numero + ", emitidos="
                    + emitidos + ", cupo=" + cupo + ", detalle=" + detalle
                    + ", lamport=" + lamport;
        }
    }

    private record EventoLog(int lamport, int idProceso, String lineaOriginal) {
        private static EventoLog desdeLinea(String linea) {
            String[] partes = linea.split(";", 4);
            if (partes.length != 4) {
                return null;
            }
            try {
                return new EventoLog(Integer.parseInt(partes[0]),
                        Integer.parseInt(partes[1]), linea);
            } catch (NumberFormatException error) {
                return null;
            }
        }
    }

    private static void registrar(int idProceso, int lamport, String evento, String detalle) {
        try {
            Files.createDirectories(CARPETA_LOGS);
            Path archivo = CARPETA_LOGS.resolve(
                    idProceso == 0 ? "coordinador.log" : "ventanilla" + idProceso + ".log");
            Object cerrojo = CERROJOS_LOG.computeIfAbsent(archivo.toAbsolutePath(), p -> new Object());
            String linea = lamport + ";" + idProceso + ";" + limpiar(evento)
                    + ";" + limpiar(detalle) + System.lineSeparator();
            synchronized (cerrojo) {
                Files.writeString(archivo, linea, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        } catch (IOException error) {
            System.err.println("No se pudo escribir el log: " + error.getMessage());
        }
    }

    private static String limpiar(String texto) {
        if (texto == null) {
            return "sin_detalle";
        }
        return texto.replace('|', '/').replace(';', ',')
                .replace('\n', ' ').replace('\r', ' ');
    }
}
