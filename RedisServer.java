import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 * A compact, Redis-style RESP2 server implemented with the Java standard library.
 * This is an educational Redis subset, not the upstream Redis server.
 */
public final class RedisServer {
    private static final int DEFAULT_PORT = 6379;
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int MAX_ARRAY_LENGTH = 1_000_000;
    private static final int MAX_BULK_LENGTH = 64 * 1024 * 1024;
    private static final int MAX_LINE_LENGTH = 64 * 1024;
    private static final int SNAPSHOT_MAGIC = 0x4a524431; // JRD1
    private static final int SNAPSHOT_VERSION = 1;

    private static final Object STORE_LOCK = new Object();
    private static final Map<String, Entry> STORE = new LinkedHashMap<>();
    private static final Map<String, Set<Client>> CHANNELS = new ConcurrentHashMap<>();
    private static final Set<Client> CLIENTS = ConcurrentHashMap.newKeySet();
    private static Path snapshotPath;
    private static volatile long lastSaveMillis;

    private RedisServer() {}

    public static void main(String[] args) throws IOException {
        if (args.length > 3) {
            System.err.println("Usage: java RedisServer [port] [host] [snapshot-file]");
            System.exit(1);
        }
        int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        String host = args.length > 1 ? args[1] : DEFAULT_HOST;
        snapshotPath = Path.of(args.length > 2 ? args[2] : "redis-data.snapshot");
        if (port < 1 || port > 65535) throw new IllegalArgumentException("Port must be 1..65535");
        loadSnapshot(snapshotPath);

        ExecutorService workers = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "redis-client");
            thread.setDaemon(true);
            return thread;
        });
        try (ServerSocket server = new ServerSocket(port, 100, InetAddress.getByName(host))) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    saveSnapshot(snapshotPath);
                } catch (IOException e) {
                    System.err.println("Could not save snapshot during shutdown: " + e.getMessage());
                }
                workers.shutdownNow();
            }));
            System.out.printf("RedisServer listening on %s:%d; snapshot: %s%n", host, port, snapshotPath);
            while (!server.isClosed()) {
                try {
                    Socket socket = server.accept();
                    Client client = new Client(socket);
                    CLIENTS.add(client);
                    workers.execute(() -> serve(client));
                } catch (SocketException e) {
                    if (!server.isClosed()) throw e;
                }
            }
        } finally {
            workers.shutdownNow();
        }
    }

    private static void serve(Client client) {
        try (Socket socket = client.socket;
             InputStream input = new BufferedInputStream(socket.getInputStream());
             OutputStream output = new BufferedOutputStream(socket.getOutputStream())) {
            client.output = output;
            while (true) {
                List<String> command = readCommand(input);
                if (command == null) break;
                Object response;
                try {
                    response = process(client, command);
                } catch (CommandException e) {
                    response = new RespError(e.getMessage());
                } catch (IOException e) {
                    response = new RespError("ERR persistence failure: " + safeMessage(e));
                } catch (RuntimeException e) {
                    response = new RespError("ERR internal server error");
                }
                if (response instanceof MultipleReplies multiple) {
                    for (Object reply : multiple.replies) client.write(reply);
                } else {
                    client.write(response);
                }
                if (!command.isEmpty() && command.get(0).equalsIgnoreCase("QUIT")) break;
            }
        } catch (IOException ignored) {
            // A client disconnect does not affect the server.
        } finally {
            unsubscribeAll(client);
            CLIENTS.remove(client);
            client.output = null;
        }
    }

    private static Object process(Client client, List<String> args) throws CommandException, IOException {
        if (args.isEmpty()) throw new CommandException("ERR empty command");
        String command = upper(args.get(0));

        if (command.equals("MULTI")) {
            requireCount(args, 1, command);
            if (client.inMulti) throw new CommandException("ERR MULTI calls can not be nested");
            client.inMulti = true;
            client.queued.clear();
            return new Simple("OK");
        }
        if (command.equals("DISCARD")) {
            requireCount(args, 1, command);
            if (!client.inMulti) throw new CommandException("ERR DISCARD without MULTI");
            client.inMulti = false;
            client.queued.clear();
            client.watched.clear();
            return new Simple("OK");
        }
        if (command.equals("WATCH")) {
            if (client.inMulti) throw new CommandException("ERR WATCH inside MULTI is not allowed");
            if (args.size() < 2) throw wrongArity(command);
            synchronized (STORE_LOCK) {
                for (int i = 1; i < args.size(); i++) {
                    String key = args.get(i);
                    client.watched.put(key, copyEntry(liveEntry(key)));
                }
            }
            return new Simple("OK");
        }
        if (command.equals("UNWATCH")) {
            requireCount(args, 1, command);
            client.watched.clear();
            return new Simple("OK");
        }
        if (command.equals("EXEC")) return executeTransaction(client, args);

        if (client.inMulti) {
            if (command.equals("SUBSCRIBE") || command.equals("UNSUBSCRIBE") || command.equals("PUBLISH")
                    || command.equals("WATCH") || command.equals("MONITOR")) {
                throw new CommandException("ERR command is not allowed inside MULTI");
            }
            client.queued.add(List.copyOf(args));
            return new Simple("QUEUED");
        }

        if (!client.subscriptions.isEmpty() && !Set.of("SUBSCRIBE", "UNSUBSCRIBE", "PING", "QUIT").contains(command)) {
            throw new CommandException("ERR only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context");
        }
        if (command.equals("SUBSCRIBE")) return subscribe(client, args);
        if (command.equals("UNSUBSCRIBE")) return unsubscribe(client, args);

        synchronized (STORE_LOCK) {
            Object response = execute(client, args);
            if (isWriteCommand(command) || command.equals("SAVE") || command.equals("BGSAVE")) saveSnapshot(snapshotPath);
            return response;
        }
    }

    private static Object executeTransaction(Client client, List<String> args) throws CommandException, IOException {
        requireCount(args, 1, "EXEC");
        if (!client.inMulti) throw new CommandException("ERR EXEC without MULTI");
        client.inMulti = false;
        List<List<String>> queued = new ArrayList<>(client.queued);
        client.queued.clear();

        synchronized (STORE_LOCK) {
            for (Map.Entry<String, Entry> watch : client.watched.entrySet()) {
                Entry current = copyEntry(liveEntry(watch.getKey()));
                if (!entriesEqual(current, watch.getValue())) {
                    client.watched.clear();
                    return NullArray.INSTANCE;
                }
            }
            client.watched.clear();
            List<Object> responses = new ArrayList<>(queued.size());
            boolean changed = false;
            for (List<String> command : queued) {
                try {
                    responses.add(execute(client, command));
                    changed |= isWriteCommand(upper(command.get(0)));
                } catch (CommandException e) {
                    responses.add(new RespError(e.getMessage()));
                }
            }
            if (changed) saveSnapshot(snapshotPath);
            return responses;
        }
    }

    private static Object execute(Client client, List<String> a) throws CommandException {
        String c = upper(a.get(0));
        switch (c) {
            case "PING":
                if (a.size() == 1) return new Simple("PONG");
                if (a.size() == 2) return a.get(1);
                throw wrongArity(c);
            case "ECHO": requireCount(a, 2, c); return a.get(1);
            case "QUIT": requireCount(a, 1, c); return new Simple("OK");
            case "SET": return set(a);
            case "GET": requireCount(a, 2, c); return stringEntry(a.get(1), false) == null ? null : stringEntry(a.get(1), false).value;
            case "GETSET": {
                requireCount(a, 3, c);
                Entry old = liveEntry(a.get(1));
                if (old != null && old.type != ValueType.STRING) throw wrongType();
                STORE.put(a.get(1), new Entry(ValueType.STRING, a.get(2), 0));
                return old == null ? null : old.value;
            }
            case "GETDEL": {
                requireCount(a, 2, c);
                Entry old = stringEntry(a.get(1), false);
                if (old == null) return null;
                STORE.remove(a.get(1));
                return old.value;
            }
            case "MGET": {
                if (a.size() < 2) throw wrongArity(c);
                List<Object> out = new ArrayList<>();
                for (int i = 1; i < a.size(); i++) {
                    Entry e = liveEntry(a.get(i));
                    out.add(e == null || e.type != ValueType.STRING ? null : e.value);
                }
                return out;
            }
            case "MSET": {
                if (a.size() < 3 || a.size() % 2 == 0) throw wrongArity(c);
                for (int i = 1; i < a.size(); i += 2) STORE.put(a.get(i), new Entry(ValueType.STRING, a.get(i + 1), 0));
                return new Simple("OK");
            }
            case "MSETNX": {
                if (a.size() < 3 || a.size() % 2 == 0) throw wrongArity(c);
                for (int i = 1; i < a.size(); i += 2) if (liveEntry(a.get(i)) != null) return 0L;
                for (int i = 1; i < a.size(); i += 2) STORE.put(a.get(i), new Entry(ValueType.STRING, a.get(i + 1), 0));
                return 1L;
            }
            case "APPEND": {
                requireCount(a, 3, c);
                Entry e = stringEntry(a.get(1), true);
                String value = (e == null ? "" : (String) e.value) + a.get(2);
                setValue(a.get(1), ValueType.STRING, value, e == null ? 0 : e.expiresAt);
                return bytes(value);
            }
            case "STRLEN": {
                requireCount(a, 2, c);
                Entry e = stringEntry(a.get(1), false);
                return e == null ? 0L : bytes((String) e.value);
            }
            case "INCR": return increment(a, 1, c);
            case "DECR": return increment(a, -1, c);
            case "INCRBY": requireCount(a, 3, c); return increment(a, parseLong(a.get(2)), c);
            case "DECRBY": requireCount(a, 3, c); return increment(a, negate(parseLong(a.get(2))), c);
            case "INCRBYFLOAT": {
                requireCount(a, 3, c);
                Entry e = stringEntry(a.get(1), true);
                double old = e == null ? 0 : parseDouble((String) e.value);
                double next = old + parseDouble(a.get(2));
                if (!Double.isFinite(next)) throw new CommandException("ERR increment would produce NaN or Infinity");
                String value = canonicalDouble(next);
                setValue(a.get(1), ValueType.STRING, value, e == null ? 0 : e.expiresAt);
                return value;
            }
            case "GETRANGE": {
                requireCount(a, 4, c);
                Entry e = stringEntry(a.get(1), false);
                String s = e == null ? "" : (String) e.value;
                int start = parseInt(a.get(2)), end = parseInt(a.get(3));
                int[] r = range(start, end, s.length());
                return r[1] < r[0] ? "" : s.substring(r[0], r[1] + 1);
            }
            case "SETRANGE": {
                requireCount(a, 4, c);
                int offset = parseInt(a.get(2));
                if (offset < 0 || offset > 512 * 1024 * 1024) throw new CommandException("ERR offset is out of range");
                Entry e = stringEntry(a.get(1), true);
                String old = e == null ? "" : (String) e.value;
                StringBuilder b = new StringBuilder(old);
                while (b.length() < offset) b.append('\0');
                String part = a.get(3);
                if (b.length() < offset + part.length()) b.setLength(offset + part.length());
                b.replace(offset, offset + part.length(), part);
                setValue(a.get(1), ValueType.STRING, b.toString(), e == null ? 0 : e.expiresAt);
                return bytes(b.toString());
            }
            case "DEL": {
                if (a.size() < 2) throw wrongArity(c);
                long count = 0;
                for (int i = 1; i < a.size(); i++) if (liveEntry(a.get(i)) != null) { STORE.remove(a.get(i)); count++; }
                return count;
            }
            case "UNLINK": return deleteKeys(a, c);
            case "EXISTS": {
                if (a.size() < 2) throw wrongArity(c);
                long count = 0;
                for (int i = 1; i < a.size(); i++) if (liveEntry(a.get(i)) != null) count++;
                return count;
            }
            case "TYPE": {
                requireCount(a, 2, c);
                Entry e = liveEntry(a.get(1));
                return new Simple(e == null ? "none" : e.type.redisName);
            }
            case "RENAME": case "RENAMENX": {
                requireCount(a, 3, c);
                Entry e = liveEntry(a.get(1));
                if (e == null) throw new CommandException("ERR no such key");
                if (c.equals("RENAMENX") && liveEntry(a.get(2)) != null) return 0L;
                if (!a.get(1).equals(a.get(2))) { STORE.put(a.get(2), e); STORE.remove(a.get(1)); }
                return c.equals("RENAME") ? new Simple("OK") : 1L;
            }
            case "EXPIRE": return expire(a, 1000L, c);
            case "PEXPIRE": return expire(a, 1L, c);
            case "EXPIREAT": return expireAt(a, 1000L, c);
            case "PEXPIREAT": return expireAt(a, 1L, c);
            case "TTL": return ttl(a, false, c);
            case "PTTL": return ttl(a, true, c);
            case "PERSIST": {
                requireCount(a, 2, c);
                Entry e = liveEntry(a.get(1));
                if (e == null || e.expiresAt == 0) return 0L;
                e.expiresAt = 0;
                return 1L;
            }
            case "KEYS": {
                requireCount(a, 2, c);
                Pattern p = glob(a.get(1));
                purgeExpired();
                List<String> keys = new ArrayList<>();
                for (String key : STORE.keySet()) if (p.matcher(key).matches()) keys.add(key);
                Collections.sort(keys);
                return keys;
            }
            case "SCAN": return scan(a);
            case "DBSIZE": requireCount(a, 1, c); purgeExpired(); return (long) STORE.size();
            case "FLUSHDB": case "FLUSHALL": {
                if (a.size() > 2) throw wrongArity(c);
                STORE.clear();
                return new Simple("OK");
            }
            case "SELECT": requireCount(a, 2, c); if (!a.get(1).equals("0")) throw new CommandException("ERR DB index is out of range"); return new Simple("OK");
            case "INFO": return info(a);
            case "LASTSAVE": requireCount(a, 1, c); return lastSaveMillis / 1000L;
            case "SAVE": requireCount(a, 1, c); return new Simple("OK"); // snapshot is written by process()
            case "BGSAVE": requireCount(a, 1, c); return new Simple("Background saving started");
            case "COMMAND": return new ArrayList<>();

            case "LPUSH": case "RPUSH": case "LPUSHX": case "RPUSHX": return listPush(a, c);
            case "LPOP": case "RPOP": return listPop(a, c);
            case "LLEN": return listLength(a, c);
            case "LRANGE": return listRange(a, c);
            case "LINDEX": return listIndex(a, c);
            case "LSET": return listSet(a, c);
            case "LINSERT": return listInsert(a, c);
            case "LTRIM": return listTrim(a, c);
            case "LREM": return listRemove(a, c);

            case "SADD": return setAdd(a, c);
            case "SREM": return setRemove(a, c);
            case "SMEMBERS": return setMembers(a, c);
            case "SISMEMBER": return setContains(a, c);
            case "SMISMEMBER": return setContainsMany(a, c);
            case "SCARD": return setCardinality(a, c);
            case "SPOP": return setPop(a, c);
            case "SUNION": case "SINTER": case "SDIFF": return setAlgebra(a, c, false);
            case "SUNIONSTORE": case "SINTERSTORE": case "SDIFFSTORE": return setAlgebra(a, c, true);
            case "SMOVE": return setMove(a, c);

            case "HSET": case "HMSET": return hashSet(a, c);
            case "HSETNX": return hashSetNx(a, c);
            case "HGET": return hashGet(a, c);
            case "HMGET": return hashMGet(a, c);
            case "HDEL": return hashDelete(a, c);
            case "HEXISTS": return hashExists(a, c);
            case "HLEN": return hashLength(a, c);
            case "HKEYS": return hashKeys(a, c);
            case "HVALS": return hashValues(a, c);
            case "HGETALL": return hashAll(a, c);
            case "HINCRBY": return hashIncrement(a, c);
            case "HINCRBYFLOAT": return hashIncrementFloat(a, c);
            case "HSTRLEN": return hashStringLength(a, c);

            case "ZADD": return zadd(a, c);
            case "ZREM": return zremove(a, c);
            case "ZSCORE": return zscore(a, c);
            case "ZMSCORE": return zmscore(a, c);
            case "ZCARD": return zcard(a, c);
            case "ZCOUNT": return zcount(a, c);
            case "ZRANK": case "ZREVRANK": return zrank(a, c);
            case "ZRANGE": case "ZREVRANGE": case "ZRANGEBYSCORE": case "ZREVRANGEBYSCORE": return zrange(a, c);
            case "ZINCRBY": return zincrby(a, c);
            case "ZREMRANGEBYRANK": return zremoveRangeRank(a, c);
            case "ZREMRANGEBYSCORE": return zremoveRangeScore(a, c);

            case "PUBLISH": return publish(a, c);
            default: throw new CommandException("ERR unknown command '" + a.get(0) + "'");
        }
    }

    private static Object set(List<String> a) throws CommandException {
        if (a.size() < 3) throw wrongArity("SET");
        boolean nx = false, xx = false, keepTtl = false;
        long expiry = 0;
        for (int i = 3; i < a.size(); i++) {
            String option = upper(a.get(i));
            switch (option) {
                case "NX": if (nx) throw syntax(); nx = true; break;
                case "XX": if (xx) throw syntax(); xx = true; break;
                case "KEEPTTL": if (keepTtl) throw syntax(); keepTtl = true; break;
                case "EX": case "PX": case "EXAT": case "PXAT": {
                    if (expiry != 0 || i + 1 >= a.size()) throw syntax();
                    long amount = parsePositiveLong(a.get(++i));
                    if (option.equals("EX")) expiry = deadline(amount, 1000L);
                    else if (option.equals("PX")) expiry = deadline(amount, 1L);
                    else if (option.equals("EXAT")) expiry = deadlineFromEpoch(amount, 1000L);
                    else expiry = deadlineFromEpoch(amount, 1L);
                    break;
                }
                default: throw syntax();
            }
        }
        if ((nx && xx) || (keepTtl && expiry != 0)) throw syntax();
        Entry old = liveEntry(a.get(1));
        if ((nx && old != null) || (xx && old == null)) return null;
        long expires = keepTtl && old != null ? old.expiresAt : expiry;
        STORE.put(a.get(1), new Entry(ValueType.STRING, a.get(2), expires));
        return new Simple("OK");
    }

    private static Object increment(List<String> a, long delta, String command) throws CommandException {
        int expected = command.equals("INCR") || command.equals("DECR") ? 2 : 3;
        requireCount(a, expected, command);
        Entry e = stringEntry(a.get(1), true);
        long old = e == null ? 0 : parseLong((String) e.value);
        long value;
        try { value = Math.addExact(old, delta); }
        catch (ArithmeticException ex) { throw new CommandException("ERR increment or decrement would overflow"); }
        setValue(a.get(1), ValueType.STRING, Long.toString(value), e == null ? 0 : e.expiresAt);
        return value;
    }

    private static Object deleteKeys(List<String> a, String command) throws CommandException {
        if (a.size() < 2) throw wrongArity(command);
        long n = 0;
        for (int i = 1; i < a.size(); i++) if (liveEntry(a.get(i)) != null) { STORE.remove(a.get(i)); n++; }
        return n;
    }

    private static Object expire(List<String> a, long unit, String c) throws CommandException {
        requireCount(a, 3, c);
        long amount = parseLong(a.get(2));
        Entry e = liveEntry(a.get(1));
        if (e == null) return 0L;
        if (amount <= 0) { STORE.remove(a.get(1)); return 1L; }
        e.expiresAt = deadline(amount, unit);
        if (e.expiresAt <= System.currentTimeMillis()) STORE.remove(a.get(1));
        return 1L;
    }

    private static Object expireAt(List<String> a, long unit, String c) throws CommandException {
        requireCount(a, 3, c);
        long epoch = parseLong(a.get(2));
        Entry e = liveEntry(a.get(1));
        if (e == null) return 0L;
        e.expiresAt = deadlineFromEpoch(epoch, unit);
        if (e.expiresAt <= System.currentTimeMillis()) STORE.remove(a.get(1));
        return 1L;
    }

    private static Object ttl(List<String> a, boolean milliseconds, String c) throws CommandException {
        requireCount(a, 2, c);
        Entry e = liveEntry(a.get(1));
        if (e == null) return -2L;
        if (e.expiresAt == 0) return -1L;
        long left = Math.max(0, e.expiresAt - System.currentTimeMillis());
        return milliseconds ? left : left / 1000L;
    }

    private static Object scan(List<String> a) throws CommandException {
        if (a.size() < 2) throw wrongArity("SCAN");
        if (!a.get(1).equals("0")) throw new CommandException("ERR cursor must be 0 in this implementation");
        String pattern = "*";
        int count = 10;
        for (int i = 2; i < a.size(); i++) {
            String opt = upper(a.get(i));
            if (opt.equals("MATCH") && i + 1 < a.size()) pattern = a.get(++i);
            else if (opt.equals("COUNT") && i + 1 < a.size()) count = Math.max(1, parseInt(a.get(++i)));
            else throw syntax();
        }
        Pattern p = glob(pattern);
        purgeExpired();
        List<String> matched = new ArrayList<>();
        for (String key : STORE.keySet()) if (p.matcher(key).matches()) matched.add(key);
        Collections.sort(matched);
        if (matched.size() > count) matched = new ArrayList<>(matched.subList(0, count));
        return List.of("0", matched);
    }

    private static Object info(List<String> a) throws CommandException {
        if (a.size() > 2) throw wrongArity("INFO");
        purgeExpired();
        long expiring = STORE.values().stream().filter(e -> e.expiresAt != 0).count();
        long strings = STORE.values().stream().filter(e -> e.type == ValueType.STRING).count();
        long lists = STORE.values().stream().filter(e -> e.type == ValueType.LIST).count();
        long sets = STORE.values().stream().filter(e -> e.type == ValueType.SET).count();
        long hashes = STORE.values().stream().filter(e -> e.type == ValueType.HASH).count();
        long zsets = STORE.values().stream().filter(e -> e.type == ValueType.ZSET).count();
        return "# Server\r\nredis_version:7.2.0-java-mini\r\nredis_mode:standalone\r\n"
                + "# Keyspace\r\ndb0:keys=" + STORE.size() + ",expires=" + expiring + "\r\n"
                + "# Types\r\nstrings=" + strings + ",lists=" + lists + ",sets=" + sets
                + ",hashes=" + hashes + ",zsets=" + zsets + "\r\n";
    }

    // Lists
    private static Object listPush(List<String> a, String c) throws CommandException {
        if (a.size() < 3) throw wrongArity(c);
        Entry e = typedEntry(a.get(1), ValueType.LIST, false);
        if (e == null && c.endsWith("X")) return 0L;
        List<String> list = e == null ? new ArrayList<>() : castList(e.value);
        if (c.startsWith("LPUSH")) for (int i = 2; i < a.size(); i++) list.add(0, a.get(i));
        else for (int i = 2; i < a.size(); i++) list.add(a.get(i));
        STORE.put(a.get(1), new Entry(ValueType.LIST, list, e == null ? 0 : e.expiresAt));
        return (long) list.size();
    }

    private static Object listPop(List<String> a, String c) throws CommandException {
        if (a.size() != 2 && a.size() != 3) throw wrongArity(c);
        Entry e = typedEntry(a.get(1), ValueType.LIST, false);
        if (e == null) return a.size() == 3 ? NullArray.INSTANCE : null;
        @SuppressWarnings("unchecked") List<String> list = (List<String>) e.value;
        if (list.isEmpty()) { STORE.remove(a.get(1)); return a.size() == 3 ? NullArray.INSTANCE : null; }
        boolean left = c.equals("LPOP");
        if (a.size() == 2) {
            String value = left ? list.remove(0) : list.remove(list.size() - 1);
            if (list.isEmpty()) STORE.remove(a.get(1));
            return value;
        }
        int count = Math.max(0, parseInt(a.get(2)));
        List<Object> out = new ArrayList<>();
        while (count-- > 0 && !list.isEmpty()) out.add(left ? list.remove(0) : list.remove(list.size() - 1));
        if (list.isEmpty()) STORE.remove(a.get(1));
        return out;
    }

    private static Object listLength(List<String> a, String c) throws CommandException {
        requireCount(a, 2, c); Entry e = typedEntry(a.get(1), ValueType.LIST, false);
        return e == null ? 0L : (long) ((List<?>) e.value).size();
    }

    private static Object listRange(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.LIST, false);
        if (e == null) return List.of();
        List<String> list = castList(e.value); int[] r = range(parseInt(a.get(2)), parseInt(a.get(3)), list.size());
        return r[1] < r[0] ? List.of() : new ArrayList<>(list.subList(r[0], r[1] + 1));
    }

    private static Object listIndex(List<String> a, String c) throws CommandException {
        requireCount(a, 3, c); Entry e = typedEntry(a.get(1), ValueType.LIST, false);
        if (e == null) return null;
        List<String> list = castList(e.value); int i = normalizeIndex(parseInt(a.get(2)), list.size());
        return i < 0 || i >= list.size() ? null : list.get(i);
    }

    private static Object listSet(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.LIST, false);
        if (e == null) throw new CommandException("ERR no such key");
        List<String> list = castList(e.value); int i = normalizeIndex(parseInt(a.get(2)), list.size());
        if (i < 0 || i >= list.size()) throw new CommandException("ERR index out of range");
        list.set(i, a.get(3)); return new Simple("OK");
    }

    private static Object listInsert(List<String> a, String c) throws CommandException {
        requireCount(a, 5, c); Entry e = typedEntry(a.get(1), ValueType.LIST, false); if (e == null) return 0L;
        List<String> list = castList(e.value); boolean before;
        if (a.get(2).equalsIgnoreCase("BEFORE")) before = true;
        else if (a.get(2).equalsIgnoreCase("AFTER")) before = false;
        else throw syntax();
        int idx = list.indexOf(a.get(3)); if (idx < 0) return -1L;
        list.add(idx + (before ? 0 : 1), a.get(4)); return (long) list.size();
    }

    private static Object listTrim(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.LIST, false); if (e == null) return new Simple("OK");
        List<String> list = castList(e.value); int[] r = range(parseInt(a.get(2)), parseInt(a.get(3)), list.size());
        List<String> keep = r[1] < r[0] ? List.of() : new ArrayList<>(list.subList(r[0], r[1] + 1));
        list.clear(); list.addAll(keep); if (list.isEmpty()) STORE.remove(a.get(1)); return new Simple("OK");
    }

    private static Object listRemove(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.LIST, false); if (e == null) return 0L;
        List<String> list = castList(e.value); long count = parseLong(a.get(2)); String target = a.get(3); long removed = 0;
        if (count >= 0) {
            for (int i = 0; i < list.size() && (count == 0 || removed < count);) {
                if (list.get(i).equals(target)) { list.remove(i); removed++; } else i++;
            }
        } else {
            for (int i = list.size() - 1; i >= 0 && removed < -count; i--) if (list.get(i).equals(target)) { list.remove(i); removed++; }
        }
        if (list.isEmpty()) STORE.remove(a.get(1)); return removed;
    }

    // Sets
    private static Object setAdd(List<String> a, String c) throws CommandException {
        if (a.size() < 3) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.SET, false);
        LinkedHashSet<String> set = e == null ? new LinkedHashSet<>() : castSet(e.value); long n = 0;
        for (int i = 2; i < a.size(); i++) if (set.add(a.get(i))) n++;
        if (!set.isEmpty()) STORE.put(a.get(1), new Entry(ValueType.SET, set, e == null ? 0 : e.expiresAt)); return n;
    }

    private static Object setRemove(List<String> a, String c) throws CommandException {
        if (a.size() < 3) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.SET, false); if (e == null) return 0L;
        Set<String> set = castSet(e.value); long n = 0; for (int i = 2; i < a.size(); i++) if (set.remove(a.get(i))) n++;
        if (set.isEmpty()) STORE.remove(a.get(1)); return n;
    }

    private static Object setMembers(List<String> a, String c) throws CommandException {
        requireCount(a, 2, c); Entry e = typedEntry(a.get(1), ValueType.SET, false); if (e == null) return List.of();
        List<String> out = new ArrayList<>(castSet(e.value)); Collections.sort(out); return out;
    }

    private static Object setContains(List<String> a, String c) throws CommandException {
        requireCount(a, 3, c); Entry e = typedEntry(a.get(1), ValueType.SET, false); return e != null && castSet(e.value).contains(a.get(2)) ? 1L : 0L;
    }

    private static Object setContainsMany(List<String> a, String c) throws CommandException {
        if (a.size() < 3) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.SET, false); List<Object> out = new ArrayList<>();
        for (int i = 2; i < a.size(); i++) out.add(e != null && castSet(e.value).contains(a.get(i)) ? 1L : 0L); return out;
    }

    private static Object setCardinality(List<String> a, String c) throws CommandException {
        requireCount(a, 2, c); Entry e = typedEntry(a.get(1), ValueType.SET, false); return e == null ? 0L : (long) castSet(e.value).size();
    }

    private static Object setPop(List<String> a, String c) throws CommandException {
        if (a.size() != 2 && a.size() != 3) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.SET, false); if (e == null) return null;
        Set<String> set = castSet(e.value); int count = a.size() == 3 ? Math.max(0, parseInt(a.get(2))) : 1; List<String> removed = new ArrayList<>();
        while (count-- > 0 && !set.isEmpty()) {
            int target = java.util.concurrent.ThreadLocalRandom.current().nextInt(set.size());
            String value = null; int index = 0;
            for (String candidate : set) { if (index++ == target) { value = candidate; break; } }
            set.remove(value); removed.add(value);
        }
        if (set.isEmpty()) STORE.remove(a.get(1)); return a.size() == 2 ? (removed.isEmpty() ? null : removed.get(0)) : removed;
    }

    private static Object setAlgebra(List<String> a, String c, boolean store) throws CommandException {
        int firstKey = store ? 2 : 1; if (a.size() <= firstKey) throw wrongArity(c);
        String destination = store ? a.get(1) : null; LinkedHashSet<String> result = null;
        for (int i = firstKey; i < a.size(); i++) {
            Entry e = typedEntry(a.get(i), ValueType.SET, false); Set<String> current = e == null ? Set.of() : castSet(e.value);
            if (result == null) result = new LinkedHashSet<>(current);
            else if (c.startsWith("SUNION")) result.addAll(current);
            else if (c.startsWith("SINTER")) result.retainAll(current);
            else result.removeAll(current);
        }
        if (result == null) result = new LinkedHashSet<>();
        if (store) { if (result.isEmpty()) STORE.remove(destination); else STORE.put(destination, new Entry(ValueType.SET, result, 0)); return (long) result.size(); }
        List<String> out = new ArrayList<>(result); Collections.sort(out); return out;
    }

    private static Object setMove(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry src = typedEntry(a.get(1), ValueType.SET, false); if (src == null || !castSet(src.value).contains(a.get(3))) return 0L;
        Entry dst = typedEntry(a.get(2), ValueType.SET, false); if (a.get(1).equals(a.get(2))) return 1L;
        castSet(src.value).remove(a.get(3)); LinkedHashSet<String> dest = dst == null ? new LinkedHashSet<>() : castSet(dst.value); dest.add(a.get(3));
        if (castSet(src.value).isEmpty()) STORE.remove(a.get(1)); if (dst != null || !dest.isEmpty()) STORE.put(a.get(2), new Entry(ValueType.SET, dest, dst == null ? 0 : dst.expiresAt)); return 1L;
    }

    // Hashes
    private static Object hashSet(List<String> a, String c) throws CommandException {
        if (a.size() < 4 || a.size() % 2 != 0) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.HASH, false);
        LinkedHashMap<String, String> map = e == null ? new LinkedHashMap<>() : castHash(e.value); long added = 0;
        for (int i = 2; i < a.size(); i += 2) { if (!map.containsKey(a.get(i))) added++; map.put(a.get(i), a.get(i + 1)); }
        STORE.put(a.get(1), new Entry(ValueType.HASH, map, e == null ? 0 : e.expiresAt)); return c.equals("HMSET") ? new Simple("OK") : added;
    }

    private static Object hashSetNx(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); LinkedHashMap<String, String> map = e == null ? new LinkedHashMap<>() : castHash(e.value);
        if (map.containsKey(a.get(2))) return 0L; map.put(a.get(2), a.get(3)); STORE.put(a.get(1), new Entry(ValueType.HASH, map, e == null ? 0 : e.expiresAt)); return 1L;
    }

    private static Object hashGet(List<String> a, String c) throws CommandException {
        requireCount(a, 3, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); return e == null ? null : castHash(e.value).get(a.get(2));
    }

    private static Object hashMGet(List<String> a, String c) throws CommandException {
        if (a.size() < 3) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); List<Object> out = new ArrayList<>();
        for (int i = 2; i < a.size(); i++) out.add(e == null ? null : castHash(e.value).get(a.get(i))); return out;
    }

    private static Object hashDelete(List<String> a, String c) throws CommandException {
        if (a.size() < 3) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); if (e == null) return 0L;
        Map<String, String> map = castHash(e.value); long n = 0; for (int i = 2; i < a.size(); i++) if (map.remove(a.get(i)) != null) n++;
        if (map.isEmpty()) STORE.remove(a.get(1)); return n;
    }

    private static Object hashExists(List<String> a, String c) throws CommandException {
        requireCount(a, 3, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); return e != null && castHash(e.value).containsKey(a.get(2)) ? 1L : 0L;
    }

    private static Object hashLength(List<String> a, String c) throws CommandException {
        requireCount(a, 2, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); return e == null ? 0L : (long) castHash(e.value).size();
    }

    private static Object hashKeys(List<String> a, String c) throws CommandException {
        requireCount(a, 2, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); return e == null ? List.of() : new ArrayList<>(castHash(e.value).keySet());
    }

    private static Object hashValues(List<String> a, String c) throws CommandException {
        requireCount(a, 2, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); return e == null ? List.of() : new ArrayList<>(castHash(e.value).values());
    }

    private static Object hashAll(List<String> a, String c) throws CommandException {
        requireCount(a, 2, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); List<Object> out = new ArrayList<>();
        if (e != null) for (Map.Entry<String, String> item : castHash(e.value).entrySet()) { out.add(item.getKey()); out.add(item.getValue()); } return out;
    }

    private static Object hashIncrement(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); Map<String, String> map = e == null ? new LinkedHashMap<>() : castHash(e.value);
        long old = map.containsKey(a.get(2)) ? parseLong(map.get(a.get(2))) : 0; long next;
        try { next = Math.addExact(old, parseLong(a.get(3))); } catch (ArithmeticException ex) { throw new CommandException("ERR increment or decrement would overflow"); }
        map.put(a.get(2), Long.toString(next)); STORE.put(a.get(1), new Entry(ValueType.HASH, map, e == null ? 0 : e.expiresAt)); return next;
    }

    private static Object hashIncrementFloat(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); Map<String, String> map = e == null ? new LinkedHashMap<>() : castHash(e.value);
        double old = map.containsKey(a.get(2)) ? parseDouble(map.get(a.get(2))) : 0; double next = old + parseDouble(a.get(3));
        if (!Double.isFinite(next)) throw new CommandException("ERR increment would produce NaN or Infinity"); String s = canonicalDouble(next);
        map.put(a.get(2), s); STORE.put(a.get(1), new Entry(ValueType.HASH, map, e == null ? 0 : e.expiresAt)); return s;
    }

    private static Object hashStringLength(List<String> a, String c) throws CommandException {
        requireCount(a, 3, c); Entry e = typedEntry(a.get(1), ValueType.HASH, false); String v = e == null ? null : castHash(e.value).get(a.get(2)); return v == null ? 0L : bytes(v);
    }

    // Sorted sets
    private static Object zadd(List<String> a, String c) throws CommandException {
        if (a.size() < 4) throw wrongArity(c); boolean nx = false, xx = false, ch = false; int i = 2;
        while (i < a.size()) { String opt = upper(a.get(i)); if (opt.equals("NX")) { nx = true; i++; } else if (opt.equals("XX")) { xx = true; i++; } else if (opt.equals("CH")) { ch = true; i++; } else if (opt.equals("GT") || opt.equals("LT")) { throw new CommandException("ERR GT/LT options are not supported"); } else break; }
        if (nx && xx || i >= a.size() || (a.size() - i) % 2 != 0) throw syntax(); Entry e = typedEntry(a.get(1), ValueType.ZSET, false);
        LinkedHashMap<String, Double> map = e == null ? new LinkedHashMap<>() : castZset(e.value); long added = 0, changed = 0;
        for (; i < a.size(); i += 2) {
            double score = parseDouble(a.get(i)); String member = a.get(i + 1); boolean exists = map.containsKey(member);
            if ((nx && exists) || (xx && !exists)) continue;
            if (!exists) added++;
            if (!exists || Double.compare(map.get(member), score) != 0) changed++;
            map.put(member, score);
        }
        if (!map.isEmpty()) STORE.put(a.get(1), new Entry(ValueType.ZSET, map, e == null ? 0 : e.expiresAt));
        return ch ? changed : added;
    }

    private static Object zremove(List<String> a, String c) throws CommandException {
        if (a.size() < 3) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.ZSET, false); if (e == null) return 0L;
        Map<String, Double> map = castZset(e.value); long n = 0; for (int i = 2; i < a.size(); i++) if (map.remove(a.get(i)) != null) n++;
        if (map.isEmpty()) STORE.remove(a.get(1)); return n;
    }

    private static Object zscore(List<String> a, String c) throws CommandException {
        requireCount(a, 3, c); Entry e = typedEntry(a.get(1), ValueType.ZSET, false); if (e == null) return null;
        Double score = castZset(e.value).get(a.get(2)); return score == null ? null : canonicalDouble(score);
    }

    private static Object zmscore(List<String> a, String c) throws CommandException {
        if (a.size() < 3) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.ZSET, false); List<Object> out = new ArrayList<>();
        for (int i = 2; i < a.size(); i++) { Double d = e == null ? null : castZset(e.value).get(a.get(i)); out.add(d == null ? null : canonicalDouble(d)); } return out;
    }

    private static Object zcard(List<String> a, String c) throws CommandException {
        requireCount(a, 2, c); Entry e = typedEntry(a.get(1), ValueType.ZSET, false); return e == null ? 0L : (long) castZset(e.value).size();
    }

    private static Object zcount(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.ZSET, false); if (e == null) return 0L;
        ScoreBound min = scoreBound(a.get(2)), max = scoreBound(a.get(3)); long n = 0;
        for (double score : castZset(e.value).values()) if (min.includesLower(score) && max.includesUpper(score)) n++; return n;
    }

    private static Object zrank(List<String> a, String c) throws CommandException {
        requireCount(a, 3, c); Entry e = typedEntry(a.get(1), ValueType.ZSET, false); if (e == null) return null;
        List<Map.Entry<String, Double>> sorted = sortedZset(castZset(e.value)); if (c.equals("ZREVRANK")) Collections.reverse(sorted);
        for (int i = 0; i < sorted.size(); i++) if (sorted.get(i).getKey().equals(a.get(2))) return (long) i; return null;
    }

    private static Object zrange(List<String> a, String c) throws CommandException {
        if (a.size() < 4) throw wrongArity(c); Entry e = typedEntry(a.get(1), ValueType.ZSET, false); if (e == null) return List.of();
        boolean withScores = false, reverse = c.startsWith("ZREVRANGE"); List<Map.Entry<String, Double>> sorted = sortedZset(castZset(e.value));
        if (c.endsWith("BYSCORE")) {
            ScoreBound low = scoreBound(a.get(2)), high = scoreBound(a.get(3));
            if (reverse) { ScoreBound t = low; low = high; high = t; Collections.reverse(sorted); }
            List<Map.Entry<String, Double>> selected = new ArrayList<>();
            for (Map.Entry<String, Double> item : sorted) if (low.includesLower(item.getValue()) && high.includesUpper(item.getValue())) selected.add(item);
            int start = 4;
            for (int i = start; i < a.size(); i++) {
                String option = upper(a.get(i));
                if (option.equals("WITHSCORES")) withScores = true;
                else if (option.equals("LIMIT") && i + 2 < a.size()) { int offset = Math.max(0, parseInt(a.get(++i))); int count = parseInt(a.get(++i)); if (offset >= selected.size()) selected.clear(); else selected = new ArrayList<>(selected.subList(offset, count < 0 ? selected.size() : Math.min(selected.size(), offset + count))); }
                else throw syntax();
            }
            return zitems(selected, withScores);
        }
        int start = parseInt(a.get(2)), stop = parseInt(a.get(3));
        for (int i = 4; i < a.size(); i++) { if (upper(a.get(i)).equals("WITHSCORES")) withScores = true; else throw syntax(); }
        if (reverse) Collections.reverse(sorted);
        int[] r = range(start, stop, sorted.size()); if (r[1] < r[0]) return List.of();
        return zitems(new ArrayList<>(sorted.subList(r[0], r[1] + 1)), withScores);
    }

    private static List<Object> zitems(List<Map.Entry<String, Double>> items, boolean scores) {
        List<Object> out = new ArrayList<>(); for (Map.Entry<String, Double> e : items) { out.add(e.getKey()); if (scores) out.add(canonicalDouble(e.getValue())); } return out;
    }

    private static Object zincrby(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); double delta = parseDouble(a.get(2)); Entry e = typedEntry(a.get(1), ValueType.ZSET, false);
        LinkedHashMap<String, Double> map = e == null ? new LinkedHashMap<>() : castZset(e.value); double next = map.getOrDefault(a.get(3), 0d) + delta;
        if (!Double.isFinite(next)) throw new CommandException("ERR resulting score is not a number (NaN)"); map.put(a.get(3), next);
        STORE.put(a.get(1), new Entry(ValueType.ZSET, map, e == null ? 0 : e.expiresAt)); return canonicalDouble(next);
    }

    private static Object zremoveRangeRank(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.ZSET, false); if (e == null) return 0L;
        List<Map.Entry<String, Double>> sorted = sortedZset(castZset(e.value)); int[] r = range(parseInt(a.get(2)), parseInt(a.get(3)), sorted.size());
        long removed = 0; if (r[1] >= r[0]) for (int i = r[0]; i <= r[1]; i++) castZset(e.value).remove(sorted.get(i).getKey());
        removed = sorted.size() - castZset(e.value).size(); if (castZset(e.value).isEmpty()) STORE.remove(a.get(1)); return removed;
    }

    private static Object zremoveRangeScore(List<String> a, String c) throws CommandException {
        requireCount(a, 4, c); Entry e = typedEntry(a.get(1), ValueType.ZSET, false); if (e == null) return 0L;
        ScoreBound min = scoreBound(a.get(2)), max = scoreBound(a.get(3)); Map<String, Double> map = castZset(e.value); int before = map.size();
        map.entrySet().removeIf(item -> min.includesLower(item.getValue()) && max.includesUpper(item.getValue())); if (map.isEmpty()) STORE.remove(a.get(1)); return (long) (before - map.size());
    }

    // Pub/sub
    private static Object subscribe(Client client, List<String> a) throws CommandException {
        if (a.size() < 2) throw wrongArity("SUBSCRIBE"); List<Object> replies = new ArrayList<>();
        for (int i = 1; i < a.size(); i++) {
            String channel = a.get(i); client.subscriptions.add(channel); CHANNELS.computeIfAbsent(channel, k -> ConcurrentHashMap.newKeySet()).add(client);
            replies.add(List.of("subscribe", channel, (long) client.subscriptions.size()));
        }
        return replies.size() == 1 ? replies.get(0) : new MultipleReplies(replies);
    }

    private static Object unsubscribe(Client client, List<String> a) {
        List<String> targets = a.size() == 1 ? new ArrayList<>(client.subscriptions) : a.subList(1, a.size());
        if (targets.isEmpty()) return java.util.Arrays.asList("unsubscribe", null, 0L);
        List<Object> responses = new ArrayList<>();
        for (String channel : targets) {
            Set<Client> subscribers = CHANNELS.get(channel); if (subscribers != null) { subscribers.remove(client); if (subscribers.isEmpty()) CHANNELS.remove(channel, subscribers); }
            client.subscriptions.remove(channel); responses.add(List.of("unsubscribe", channel, (long) client.subscriptions.size()));
        }
        return responses.size() == 1 ? responses.get(0) : new MultipleReplies(responses);
    }

    private static Object publish(List<String> a, String c) throws CommandException {
        requireCount(a, 3, c); Set<Client> subscribers = CHANNELS.getOrDefault(a.get(1), Set.of());
        int delivered = 0;
        for (Client client : subscribers) { client.write(List.of("message", a.get(1), a.get(2))); delivered++; }
        return (long) delivered;
    }

    private static void unsubscribeAll(Client client) {
        for (String channel : new ArrayList<>(client.subscriptions)) {
            Set<Client> subscribers = CHANNELS.get(channel);
            if (subscribers != null) { subscribers.remove(client); if (subscribers.isEmpty()) CHANNELS.remove(channel, subscribers); }
        }
        client.subscriptions.clear();
    }

    // Store helpers
    private static Entry liveEntry(String key) {
        Entry e = STORE.get(key);
        if (e != null && e.expiresAt != 0 && e.expiresAt <= System.currentTimeMillis()) { STORE.remove(key); return null; }
        return e;
    }

    private static boolean isWriteCommand(String command) {
        return Set.of(
                "SET", "GETSET", "GETDEL", "MSET", "MSETNX", "APPEND", "INCR", "DECR", "INCRBY", "DECRBY",
                "INCRBYFLOAT", "SETRANGE", "DEL", "UNLINK", "RENAME", "RENAMENX", "EXPIRE", "PEXPIRE",
                "EXPIREAT", "PEXPIREAT", "PERSIST", "FLUSHDB", "FLUSHALL",
                "LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LSET", "LINSERT", "LTRIM", "LREM",
                "SADD", "SREM", "SPOP", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE", "SMOVE",
                "HSET", "HMSET", "HSETNX", "HDEL", "HINCRBY", "HINCRBYFLOAT",
                "ZADD", "ZREM", "ZINCRBY", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE"
        ).contains(command);
    }

    private static Entry typedEntry(String key, ValueType type, boolean onlyIfExists) throws CommandException {
        Entry e = liveEntry(key);
        if (e == null) return null;
        if (e.type != type) throw wrongType();
        return e;
    }

    private static Entry stringEntry(String key, boolean createAllowed) throws CommandException {
        Entry e = liveEntry(key); if (e != null && e.type != ValueType.STRING) throw wrongType(); return e;
    }

    private static void setValue(String key, ValueType type, Object value, long expiresAt) {
        STORE.put(key, new Entry(type, value, expiresAt));
    }

    private static void purgeExpired() {
        long now = System.currentTimeMillis(); STORE.entrySet().removeIf(e -> e.getValue().expiresAt != 0 && e.getValue().expiresAt <= now);
    }

    private static Entry copyEntry(Entry e) { return e == null ? null : new Entry(e.type, deepCopy(e.value), e.expiresAt); }
    private static Object deepCopy(Object value) {
        if (value instanceof List<?> list) return new ArrayList<>(list);
        if (value instanceof Set<?> set) return new LinkedHashSet<>(set);
        if (value instanceof Map<?, ?> map) return new LinkedHashMap<>(map);
        return value;
    }
    private static boolean entriesEqual(Entry left, Entry right) {
        return left == null ? right == null : right != null && left.type == right.type && left.expiresAt == right.expiresAt && left.value.equals(right.value);
    }

    @SuppressWarnings("unchecked") private static List<String> castList(Object value) { return (List<String>) value; }
    @SuppressWarnings("unchecked") private static LinkedHashSet<String> castSet(Object value) { return (LinkedHashSet<String>) value; }
    @SuppressWarnings("unchecked") private static LinkedHashMap<String, String> castHash(Object value) { return (LinkedHashMap<String, String>) value; }
    @SuppressWarnings("unchecked") private static LinkedHashMap<String, Double> castZset(Object value) { return (LinkedHashMap<String, Double>) value; }

    private static int[] range(int start, int stop, int size) {
        int s = normalizeIndex(start, size), e = normalizeIndex(stop, size);
        s = Math.max(0, s); e = Math.min(size - 1, e); return new int[]{s, e};
    }
    private static int normalizeIndex(int index, int size) { return index < 0 ? size + index : index; }

    private static List<Map.Entry<String, Double>> sortedZset(Map<String, Double> map) {
        List<Map.Entry<String, Double>> out = new ArrayList<>(map.entrySet());
        out.sort(Comparator.<Map.Entry<String, Double>>comparingDouble(Map.Entry::getValue).thenComparing(Map.Entry::getKey)); return out;
    }

    private static Pattern glob(String s) {
        StringBuilder r = new StringBuilder("^");
        for (char ch : s.toCharArray()) {
            if (ch == '*') r.append(".*"); else if (ch == '?') r.append('.');
            else { if ("\\.[]{}()+-^$|".indexOf(ch) >= 0) r.append('\\'); r.append(ch); }
        }
        return Pattern.compile(r.append('$').toString(), Pattern.DOTALL);
    }

    private static long bytes(String s) { return s.getBytes(StandardCharsets.UTF_8).length; }
    private static String upper(String s) { return s.toUpperCase(Locale.ROOT); }
    private static String safeMessage(IOException e) { return e.getMessage() == null ? "I/O error" : e.getMessage(); }
    private static CommandException wrongType() { return new CommandException("WRONGTYPE Operation against a key holding the wrong kind of value"); }
    private static CommandException syntax() { return new CommandException("ERR syntax error"); }
    private static CommandException wrongArity(String c) { return new CommandException("ERR wrong number of arguments for '" + c.toLowerCase(Locale.ROOT) + "' command"); }
    private static void requireCount(List<String> a, int count, String c) throws CommandException { if (a.size() != count) throw wrongArity(c); }

    private static int parseInt(String s) throws CommandException {
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { throw new CommandException("ERR value is not an integer or out of range"); }
    }
    private static long parseLong(String s) throws CommandException {
        try { return Long.parseLong(s); } catch (NumberFormatException e) { throw new CommandException("ERR value is not an integer or out of range"); }
    }
    private static long parsePositiveLong(String s) throws CommandException { long n = parseLong(s); if (n <= 0) throw new CommandException("ERR invalid expire time"); return n; }
    private static long negate(long n) throws CommandException { if (n == Long.MIN_VALUE) throw new CommandException("ERR increment or decrement would overflow"); return -n; }
    private static double parseDouble(String s) throws CommandException {
        try { double d = Double.parseDouble(s); if (!Double.isFinite(d)) throw new NumberFormatException(); return d; }
        catch (NumberFormatException e) { throw new CommandException("ERR value is not a valid float"); }
    }
    private static String canonicalDouble(double n) { if (n == 0d) return "0"; return Double.toString(n); }
    private static long deadline(long amount, long unit) {
        try { return Math.addExact(System.currentTimeMillis(), Math.multiplyExact(amount, unit)); }
        catch (ArithmeticException e) { return Long.MAX_VALUE; }
    }
    private static long deadlineFromEpoch(long amount, long unit) {
        try { return Math.multiplyExact(amount, unit); } catch (ArithmeticException e) { return amount < 0 ? Long.MIN_VALUE : Long.MAX_VALUE; }
    }

    private static ScoreBound scoreBound(String raw) throws CommandException {
        boolean exclusive = raw.startsWith("("); String value = exclusive ? raw.substring(1) : raw;
        if (value.equalsIgnoreCase("-inf")) return new ScoreBound(Double.NEGATIVE_INFINITY, exclusive, true);
        if (value.equalsIgnoreCase("+inf") || value.equalsIgnoreCase("inf")) return new ScoreBound(Double.POSITIVE_INFINITY, exclusive, false);
        return new ScoreBound(parseDouble(value), exclusive, false);
    }

    // Snapshot persistence. Each write command saves a consistent snapshot using atomic replacement.
    private static void saveSnapshot(Path path) throws IOException {
        if (path == null) return;
        synchronized (STORE_LOCK) {
        purgeExpired();
        Path absolute = path.toAbsolutePath(); Path parent = absolute.getParent(); if (parent != null) Files.createDirectories(parent);
        Path temp = absolute.resolveSibling(absolute.getFileName() + ".tmp");
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(temp)))) {
            out.writeInt(SNAPSHOT_MAGIC); out.writeInt(SNAPSHOT_VERSION); out.writeInt(STORE.size());
            for (Map.Entry<String, Entry> item : STORE.entrySet()) {
                writeText(out, item.getKey()); Entry e = item.getValue(); out.writeByte(e.type.ordinal()); out.writeLong(e.expiresAt);
                switch (e.type) {
                    case STRING: writeText(out, (String) e.value); break;
                    case LIST: { List<String> list = castList(e.value); out.writeInt(list.size()); for (String v : list) writeText(out, v); break; }
                    case SET: { Set<String> set = castSet(e.value); out.writeInt(set.size()); for (String v : set) writeText(out, v); break; }
                    case HASH: { Map<String, String> map = castHash(e.value); out.writeInt(map.size()); for (Map.Entry<String, String> v : map.entrySet()) { writeText(out, v.getKey()); writeText(out, v.getValue()); } break; }
                    case ZSET: { Map<String, Double> map = castZset(e.value); out.writeInt(map.size()); for (Map.Entry<String, Double> v : map.entrySet()) { writeText(out, v.getKey()); out.writeDouble(v.getValue()); } break; }
                }
            }
            out.flush();
        }
        try { Files.move(temp, absolute, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE); }
        catch (AtomicMoveNotSupportedException e) { Files.move(temp, absolute, StandardCopyOption.REPLACE_EXISTING); }
        lastSaveMillis = System.currentTimeMillis();
        }
    }

    private static void loadSnapshot(Path path) throws IOException {
        if (!Files.exists(path)) return;
        synchronized (STORE_LOCK) {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
                if (in.readInt() != SNAPSHOT_MAGIC) throw new IOException("Not a RedisServer snapshot: " + path);
                int version = in.readInt(); if (version != SNAPSHOT_VERSION) throw new IOException("Unsupported snapshot version: " + version);
                int count = readCount(in);
                for (int i = 0; i < count; i++) {
                    String key = readText(in); int typeNumber = in.readUnsignedByte();
                    if (typeNumber >= ValueType.values().length) throw new IOException("Invalid snapshot value type");
                    ValueType type = ValueType.values()[typeNumber]; long expiresAt = in.readLong(); Object value;
                    switch (type) {
                        case STRING: value = readText(in); break;
                        case LIST: { int n = readCount(in); List<String> v = new ArrayList<>(); for (int j = 0; j < n; j++) v.add(readText(in)); value = v; break; }
                        case SET: { int n = readCount(in); LinkedHashSet<String> v = new LinkedHashSet<>(); for (int j = 0; j < n; j++) v.add(readText(in)); value = v; break; }
                        case HASH: { int n = readCount(in); LinkedHashMap<String, String> v = new LinkedHashMap<>(); for (int j = 0; j < n; j++) v.put(readText(in), readText(in)); value = v; break; }
                        case ZSET: { int n = readCount(in); LinkedHashMap<String, Double> v = new LinkedHashMap<>(); for (int j = 0; j < n; j++) v.put(readText(in), in.readDouble()); value = v; break; }
                        default: throw new IOException("Unsupported snapshot value type");
                    }
                    if (expiresAt == 0 || expiresAt > System.currentTimeMillis()) STORE.put(key, new Entry(type, value, expiresAt));
                }
            } catch (EOFException e) { throw new IOException("Snapshot is truncated: " + path, e); }
        }
        lastSaveMillis = Files.getLastModifiedTime(path).toMillis();
    }

    private static int readCount(DataInputStream in) throws IOException { int n = in.readInt(); if (n < 0 || n > 10_000_000) throw new IOException("Invalid snapshot item count"); return n; }
    private static void writeText(DataOutputStream out, String text) throws IOException { byte[] b = text.getBytes(StandardCharsets.UTF_8); out.writeInt(b.length); out.write(b); }
    private static String readText(DataInputStream in) throws IOException { int n = in.readInt(); if (n < 0 || n > MAX_BULK_LENGTH) throw new IOException("Invalid snapshot string length"); byte[] b = in.readNBytes(n); if (b.length != n) throw new EOFException(); return new String(b, StandardCharsets.UTF_8); }

    // RESP2 request and response handling
    private static List<String> readCommand(InputStream in) throws IOException {
        int first = in.read(); if (first < 0) return null;
        if (first == '*') {
            int count = readLength(readLine(in), "array length"); if (count < 1 || count > MAX_ARRAY_LENGTH) throw new IOException("Invalid command array length");
            List<String> args = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                if (in.read() != '$') throw new IOException("Command arguments must be bulk strings");
                int length = readLength(readLine(in), "bulk string length"); if (length < 0 || length > MAX_BULK_LENGTH) throw new IOException("Invalid bulk string length");
                byte[] value = in.readNBytes(length); if (value.length != length) throw new EOFException(); expectCrlf(in); args.add(new String(value, StandardCharsets.UTF_8));
            }
            return args;
        }
        String line = readLine(in, first).trim(); return line.isEmpty() ? List.of() : List.of(line.split("\\s+"));
    }

    private static int readLength(String s, String what) throws IOException { try { return Integer.parseInt(s); } catch (NumberFormatException e) { throw new IOException("Invalid " + what, e); } }
    private static String readLine(InputStream in) throws IOException { int first = in.read(); if (first < 0) throw new EOFException(); return readLine(in, first); }
    private static String readLine(InputStream in, int first) throws IOException {
        byte[] buffer = new byte[MAX_LINE_LENGTH]; int n = 0, current = first;
        while (true) {
            if (current < 0) throw new EOFException();
            if (current == '\r') { if (in.read() != '\n') throw new IOException("Expected CRLF"); return new String(buffer, 0, n, StandardCharsets.UTF_8); }
            if (n == buffer.length) throw new IOException("Line too long"); buffer[n++] = (byte) current; current = in.read();
        }
    }
    private static void expectCrlf(InputStream in) throws IOException { if (in.read() != '\r' || in.read() != '\n') throw new IOException("Expected CRLF"); }

    private static void writeResponse(OutputStream out, Object response) throws IOException {
        if (response == null) { writeAscii(out, "$-1\r\n"); return; }
        if (response == NullArray.INSTANCE) { writeAscii(out, "*-1\r\n"); return; }
        if (response instanceof Simple s) { writeLine(out, '+', s.value); return; }
        if (response instanceof RespError e) { writeLine(out, '-', e.message); return; }
        if (response instanceof Number n) { writeAscii(out, ":" + n.longValue() + "\r\n"); return; }
        if (response instanceof String s) { byte[] b = s.getBytes(StandardCharsets.UTF_8); writeAscii(out, "$" + b.length + "\r\n"); out.write(b); writeAscii(out, "\r\n"); return; }
        if (response instanceof List<?> list) { writeAscii(out, "*" + list.size() + "\r\n"); for (Object item : list) writeResponse(out, item); return; }
        throw new IOException("Unsupported response type: " + response.getClass().getName());
    }
    private static void writeLine(OutputStream out, char prefix, String text) throws IOException { writeAscii(out, prefix + text.replace("\r", " ").replace("\n", " ") + "\r\n"); }
    private static void writeAscii(OutputStream out, String s) throws IOException { out.write(s.getBytes(StandardCharsets.UTF_8)); }

    private enum ValueType {
        STRING("string"), LIST("list"), SET("set"), HASH("hash"), ZSET("zset");
        final String redisName; ValueType(String redisName) { this.redisName = redisName; }
    }
    private static final class Entry {
        final ValueType type; final Object value; long expiresAt;
        Entry(ValueType type, Object value, long expiresAt) { this.type = type; this.value = value; this.expiresAt = expiresAt; }
    }
    private record Simple(String value) {}
    private record RespError(String message) {}
    private record MultipleReplies(List<Object> replies) {}
    private enum NullArray { INSTANCE }
    private record ScoreBound(double value, boolean exclusive, boolean positiveInfinity) {
        boolean includesLower(double score) { int c = Double.compare(score, value); return c > 0 || (c == 0 && !exclusive); }
        boolean includesUpper(double score) { int c = Double.compare(score, value); return c < 0 || (c == 0 && !exclusive); }
    }
    private static final class Client {
        final Socket socket; volatile OutputStream output; final Set<String> subscriptions = ConcurrentHashMap.newKeySet();
        final List<List<String>> queued = new ArrayList<>(); final Map<String, Entry> watched = new LinkedHashMap<>(); boolean inMulti;
        Client(Socket socket) { this.socket = socket; }
        void write(Object value) { OutputStream out = output; if (out == null) return; synchronized (this) { try { writeResponse(out, value); out.flush(); } catch (IOException ignored) {} } }
    }
    private static final class CommandException extends Exception { CommandException(String message) { super(message); } }
}
