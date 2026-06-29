var communicatie = (function () {

    var _bp = (typeof _base_path !== "undefined") ? _base_path : "";

    // ── State ──────────────────────────────────────────────────────────
    var socket;
    var user_id   = "";
    var user_name = "";
    var is_owner  = false;

    var _profile_target = null;   // null = own profile, else user object from users list

    var current_channel_id   = null;
    var current_channel_peer = null;
    var _chat_origin         = "stream"; // nav page to return to on back
    var pending_parent_id    = null;
    var pending_image        = null;
    var pending_scroll_id    = null;

    var known_profiles  = {};
    var needed_profiles = new Set();
    var _stream_items    = [];
    var _stream_has_more = false;
    var _stream_loading  = false;
    var _stream_observer = null;
    var _stream_muted      = new Set(); // private remote channels opted OUT
    var _stream_subscribed = new Set(); // public  remote channels opted IN
    var _blocked_users     = new Set(); // "user_id|peer_id" — never show messages from these users

    function _save_stream_muted()      { send("set_user_setting", { key: "stream_muted",      value: Array.from(_stream_muted) }); }
    function _save_stream_subscribed() { send("set_user_setting", { key: "stream_subscribed", value: Array.from(_stream_subscribed) }); }
    function _save_blocked_users()     { send("set_user_setting", { key: "blocked_users",     value: Array.from(_blocked_users) }); }

    function _block_key(user_id, peer_id) { return (user_id || "") + "|" + (peer_id || ""); }
    function _is_blocked(msg) { return _blocked_users.has(_block_key(msg.sender_user_id, msg.sender_peer_id)); }

    // ── WebSocket ──────────────────────────────────────────────────────
    var _queue = [];

    function connect() {
        socket = new WebSocket("wss://" + location.host + _bp + "/ws");

        socket.onopen = function () {
            _queue.forEach(function (msg) { socket.send(msg); });
            _queue = [];
        };

        socket.onmessage = function (event) {
            dispatch(JSON.parse(event.data));
        };

        socket.onclose = function () {
            setTimeout(connect, 3000);
        };

        socket.onerror = function () {};
    }

    function send(type, params) {
        var msg = JSON.stringify(Object.assign({ type: type }, params || {}));
        if (socket && socket.readyState === WebSocket.OPEN) {
            socket.send(msg);
        } else {
            _queue.push(msg);
        }
    }

    var _pending = {};

    function request(type, params) {
        return new Promise(function (resolve) {
            _pending[type] = _pending[type] || [];
            _pending[type].push(resolve);
            send(type, params);
        });
    }

    function dispatch(data) {
        if (data.type === "auth" && !data.ok) {
            handle_auth_error();
            return;
        }
        var resolvers = _pending[data.type];
        if (resolvers && resolvers.length) resolvers.shift()(data);
        if (data.type === "message"        && data.ok) on_live_message(data.message);
        if (data.type === "stream_message" && data.ok) on_stream_message(data.message);
        if (data.type === "peer_added"      && data.ok) setup_stream_page();
        if (data.type === "stream_update"  && data.ok) {
            if (data.has_more) _stream_has_more = true;   // set BEFORE render
            on_stream_update(data.messages);
        }
        if (data.type === "read_stream"    && data.ok) {
            _stream_has_more = !!data.has_more;           // set BEFORE render
            _stream_loading  = false;
            on_stream_update(data.messages);
        }
    }

    // ── Auth error → login ─────────────────────────────────────────────
    function handle_auth_error() {
        show_page("/login.html", setup_login_page);
    }

    // ── Error toast ────────────────────────────────────────────────────
    function show_error(msg) {
        var el = document.getElementById("error");
        el.textContent = msg;
        el.classList.remove("hidden");
        setTimeout(function () { el.classList.add("hidden"); }, 5000);
    }

    // ── Safe DOM helpers ───────────────────────────────────────────────
function _channel_icon(ch) {
        return ch.icon || (ch.public ? "🌐" : "🔒");
    }

    function make(tag, cls, text) {
        var el = document.createElement(tag);
        if (cls)  el.className = cls;
        if (text) el.appendChild(document.createTextNode(text));
        return el;
    }

    function avatar_img(src, cls) {
        var img = document.createElement("img");
        img.className = cls || "avatar";
        img.src = src
            ? (src.startsWith("http") || src.startsWith("/")) ? src : _bp + "/img/" + src
            : "";
        img.alt = "";
        return img;
    }

    function fmt_time(iso) {
        if (!iso) return "";
        var d = new Date(iso);
        return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    }

    function fmt_relative(iso) {
        if (!iso) return "";
        var d   = new Date(iso);
        var now = new Date();
        var diff = now - d;
        if (diff < 60000)    return "now";
        if (diff < 3600000)  return Math.floor(diff / 60000) + "m";
        if (diff < 86400000) return fmt_time(iso);
        if (diff < 604800000)
            return ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"][d.getDay()];
        return d.toLocaleDateString([], { month: "short", day: "numeric" });
    }

    // ── Page loading ───────────────────────────────────────────────────
    var _page_setup = {
        "/stream.html":   setup_stream_page,
        "/peers.html":    setup_peers_page,
        "/server.html":   setup_server_page,
        "/advanced.html": setup_advanced_page,
        "/users.html":    setup_users_page,
        "/channels.html": setup_channels_page,
        "/user.html":     setup_user_page,
        "/login.html":    setup_login_page,
    };

    function show_page(url, callback) {
        fetch(_bp + url)
            .then(function (r) { return r.text(); })
            .then(function (html) {
                // Safe: templates contain only static HTML, no user data.
                document.getElementById("main").innerHTML = html;
                var setup = _page_setup[url];
                if (setup)    setup();
                if (callback) callback();
            })
            .catch(function () { show_error("Could not load page"); });
    }

    // ── Nav badges ────────────────────────────────────────────────────
    function set_nav_badge(page, label) {
        document.querySelectorAll(".nav-item[data-page='" + page + "']").forEach(function (el) {
            var existing = el.querySelector(".nav-badge");
            if (label) {
                if (!existing) { existing = make("span", "nav-badge"); el.appendChild(existing); }
                existing.textContent = label;
            } else if (existing) {
                existing.remove();
            }
        });
    }

    function _cert_expiring_soon(expires_str, valid_days) {
        if (!expires_str) return false;
        var remaining_days = (new Date(expires_str) - new Date()) / 86400000;
        var threshold_days = Math.max(7, (valid_days || 3650) * 90 / 3650);
        return remaining_days < threshold_days;
    }

    function _check_nav_badges() {
        if (!is_owner) return;
        request("read_cert_config").then(function (d) {
            if (!d.ok || !d.info) return;
            set_nav_badge("server", _cert_expiring_soon(d.info.expires, d.info.valid_days) ? "!" : "");
        });
        request("read_peers").then(function (d) {
            if (!d.ok) return;
            var pending = (d.peers || []).filter(function (p) { return p.status === "pending"; }).length;
            set_nav_badge("peers", pending ? String(pending) : "");
        });
    }

    // ── Navigation ─────────────────────────────────────────────────────
    var _page_map = {
        "stream":   "/stream.html",
        "peers":    "/peers.html",
        "server":   "/server.html",
        "users":    "/users.html",
        "channels": "/channels.html",
        "user":     "/user.html",
    };

    function setup_nav() {
        document.querySelectorAll(".nav-item[data-page]").forEach(function (el) {
            el.addEventListener("click", function () {
                var page = el.dataset.page;
                if (!_page_map[page]) return;
                set_active_nav(page);
                exit_chat();
                show_page(_page_map[page]);
            });
        });
    }

    function set_active_nav(page) {
        document.querySelectorAll(".nav-item").forEach(function (el) {
            el.classList.toggle("active", el.dataset.page === page);
        });
    }

    function exit_chat() {
        document.body.classList.remove("in-chat");
        current_channel_id   = null;
        current_channel_peer = null;
        document.title = "Messages";
        send("unsubscribe_all");
    }

    // ── Init ───────────────────────────────────────────────────────────
    function init(logged_in, uid, uname, owner) {
        user_id   = uid;
        user_name = uname;
        is_owner  = owner;

        document.addEventListener("click", function () {
            document.querySelectorAll(".info-btn.open").forEach(function (b) { b.classList.remove("open"); });
        });

        if (logged_in) {
            connect();
            // Load user settings (queued until WS opens, resolves before stream messages)
            request("read_user_settings").then(function (d) {
                if (!d.ok || !d.settings) return;
                var muted = d.settings.stream_muted;
                if (Array.isArray(muted)) muted.forEach(function (k) { _stream_muted.add(k); });
                var sub = d.settings.stream_subscribed;
                if (Array.isArray(sub)) sub.forEach(function (k) { _stream_subscribed.add(k); });
                var blocked = d.settings.blocked_users;
                if (Array.isArray(blocked)) blocked.forEach(function (k) { _blocked_users.add(k); });
            });
            setup_nav();
            set_active_nav("stream");
            _check_nav_badges();

            // Seed own profile so stream shows our name immediately
            known_profiles[user_id] = { id: user_id, name: user_name };
            request("read_user", { id: user_id }).then(function (d) {
                if (!d.ok || !d.user) return;
                known_profiles[user_id] = d.user;
                if (d.user.avatar) {
                    var img = document.getElementById("nav-avatar");
                    if (img) img.src = _bp + "/img/" + d.user.avatar;
                }
            });

            show_page("/stream.html");
        } else {
            show_page("/login.html");
        }
    }

    // ── Login page ─────────────────────────────────────────────────────
    function setup_login_page() {
        document.getElementById("login_form").addEventListener("submit", function (e) {
            e.preventDefault();
            do_login();
        });
        document.getElementById("register_form").addEventListener("submit", function (e) {
            e.preventDefault();
            do_register();
        });
    }

    function do_login() {
        var name     = document.getElementById("login_name").value.trim();
        var password = document.getElementById("login_password").value;
        if (!name || !password) return;

        fetch(_bp + "/login", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: name, password: password }),
        })
            .then(function (r) { return r.json(); })
            .then(function (d) {
                if (d.ok) location.reload();
                else show_error(d.reason || "Login failed");
            })
            .catch(function () { show_error("Login error"); });
    }

    function do_register() {
        var name     = document.getElementById("register_name").value.trim();
        var password = document.getElementById("register_password").value;
        if (!name || !password) return;

        fetch(_bp + "/register", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: name, password: password }),
        })
            .then(function (r) { return r.json(); })
            .then(function (d) {
                if (d.ok) {
                    document.getElementById("login_name").value     = name;
                    document.getElementById("login_password").value = password;
                    do_login();
                } else {
                    show_error(d.reason || "Registration failed");
                }
            })
            .catch(function () { show_error("Registration error"); });
    }

    // ── Stream page ────────────────────────────────────────────────────
    function render_stream(anchor_top) {
        // anchor_top: when true, adjust scrollTop so existing content stays in view
        // (use for live messages added at top). False/omit for bottom additions (lazy load).
        var list = document.getElementById("stream-list");
        if (!list) return;
        var prev_top = list.scrollTop || 0;
        var prev_h   = list.scrollHeight || 0;
        list.replaceChildren();
        if (!_stream_items.length) {
            list.appendChild(make("p", "stream-empty",
                "No messages yet — create a channel and start chatting."));
            return;
        }

        // Sort newest-first; group only consecutive same-channel messages
        var sorted = _stream_items.slice().sort(function (a, b) { return b.t - a.t; });
        var last_key = null;
        sorted.forEach(function (item) {
            var msg = item.data;
            var key = (msg.channel_id || "") + "|" + (msg.peer_id || "");
            list.appendChild(build_stream_message(msg, key === last_key));
            last_key = key;
        });

        // Bottom sentinel / end-of-stream indicator
        if (_stream_has_more) {
            var sentinel = make("div", "stream-sentinel");
            list.appendChild(sentinel);
            if (_stream_observer) _stream_observer.disconnect();
            // Use window as root when list doesn't scroll (content shorter than viewport)
            var use_window = list.scrollHeight <= list.clientHeight;
            _stream_observer = new IntersectionObserver(function (entries) {
                if (entries[0].isIntersecting) _stream_load_more();
            }, use_window ? { rootMargin: "0px 0px 200px 0px" }
                          : { root: list, rootMargin: "0px 0px 150px 0px" });
            _stream_observer.observe(sentinel);
        } else if (_stream_items.length) {
            list.appendChild(make("div", "stream-end", "— begin van de stroom —"));
        }

        // Anchor existing content when new messages appear above (live updates at top).
        // For lazy-loaded older messages (added at bottom), scrollTop stays unchanged.
        if (anchor_top && prev_top > 0) {
            var new_h = list.scrollHeight;
            requestAnimationFrame(function () {
                list.scrollTop = prev_top + (new_h - prev_h);
            });
        }
    }

    function _stream_seen() {
        var s = new Set();
        _stream_items.forEach(function (i) { s.add(i.data.id); });
        return s;
    }

    function _stream_muted_key(msg) {
        return (msg.channel_id || "") + "|" + (msg.peer_id || "");
    }

    function _stream_visible(msg) {
        if (_is_blocked(msg)) return false;
        var key = _stream_muted_key(msg);
        if (!msg.peer_id) return true;            // local channel: server handles stream_excluded
        return msg.channel_public
            ? _stream_subscribed.has(key)         // public remote: must be opted in
            : !_stream_muted.has(key);            // private remote: unless muted
    }

    function on_stream_message(msg) {
        if (!_stream_visible(msg)) return;
        if (!_stream_seen().has(msg.id)) {
            _stream_items.push({ t: new Date(msg.created), data: msg });
            render_stream(true); // live message appears at top → anchor existing content
        }
    }

    function on_stream_update(messages) {
        var seen = _stream_seen();
        var added = false;
        (messages || []).forEach(function (msg) {
            if (!_stream_visible(msg)) return;
            if (!seen.has(msg.id)) { seen.add(msg.id); _stream_items.push({ t: new Date(msg.created), data: msg }); added = true; }
        });
        if (added) render_stream(); // items at bottom or initial → no scroll anchor
    }

    function _stream_load_more() {
        if (!_stream_has_more || _stream_loading) return;

        // Per-source cursors: local uses its own oldest ts, each remote channel its own.
        var local_before = null;
        var remote_cursors = {};  // { "channel_id|peer_id": iso_string }

        _stream_items.forEach(function (item) {
            var msg = item.data;
            if (!msg.peer_id) {
                if (!local_before || item.t < local_before) local_before = item.t;
            } else {
                var key = (msg.channel_id || "") + "|" + (msg.peer_id || "");
                if (!remote_cursors[key] || item.t < new Date(remote_cursors[key]))
                    remote_cursors[key] = item.t.toISOString();
            }
        });

        if (!local_before && !Object.keys(remote_cursors).length) return;

        _stream_loading = true;
        var params = { paginate: true };
        if (local_before)                          params.local_before    = local_before.toISOString();
        if (Object.keys(remote_cursors).length)    params.remote_cursors  = remote_cursors;
        send("read_stream", params);
    }

    function setup_stream_page() {
        _stream_items    = [];
        _stream_has_more = false;
        _stream_loading  = false;
        if (_stream_observer) { _stream_observer.disconnect(); _stream_observer = null; }
        send("read_stream");
    }

    function build_stream_message(msg, is_cont) {
        var div = make("div", "stream-item" + (is_cont ? " stream-cont" : ""));

        if (!is_cont) {
            var badge = make("div", "stream-badge");
            badge.appendChild(make("span", null, msg.channel_icon || (msg.channel_public ? "🌐" : "🔒")));
            badge.appendChild(make("span", "stream-badge-name", msg.channel_name));
            if (msg.peer_name) badge.appendChild(make("span", "stream-peer", " @ " + msg.peer_name));
            var mute_key = (msg.channel_id || "") + "|" + (msg.peer_id || "");
            var tbtn = make("span", "stream-badge-toggle", "🔕");
            tbtn.title = "Remove from stream";
            tbtn.addEventListener("click", function (e) {
                e.stopPropagation();
                if (!msg.peer_id) {
                    // Local channel: server-side toggle
                    request("toggle_stream_channel", { channel_id: msg.channel_id }).then(function (d) {
                        if (!d.ok) { show_error(d.reason); return; }
                        if (d.stream_excluded) {
                            _stream_items = _stream_items.filter(function (i) {
                                return i.data.channel_id !== msg.channel_id;
                            });
                            render_stream();
                        }
                    });
                } else {
                    // Remote channel: toggle via subscribed/muted depending on public
                    if (msg.channel_public) {
                        _stream_subscribed.delete(mute_key);
                        _save_stream_subscribed();
                    } else {
                        _stream_muted.add(mute_key);
                        _save_stream_muted();
                    }
                    _stream_items = _stream_items.filter(function (i) { return _stream_visible(i.data); });
                    render_stream();
                }
            });
            badge.appendChild(tbtn);
            div.appendChild(badge);
        }

        var row = make("div", "stream-row");
        row.appendChild(avatar_img(
            msg.sender_avatar || (known_profiles[msg.sender_user_id] || {}).avatar
        ));

        var body = make("div", "stream-body");
        var top = make("div", "stream-top");
        var is_remote_sender = !!msg.sender_peer_id;
        var sname = msg.sender_name
            || (known_profiles[msg.sender_user_id] || {}).name
            || (is_remote_sender ? "@ " + (msg.peer_name || "?") : "…");
        var sname_el = make("span", "stream-name", sname);
        if (!is_remote_sender && msg.sender_user_id) {
            sname_el.dataset.uid = msg.sender_user_id;
            if (!msg.sender_name && !known_profiles[msg.sender_user_id])
                needed_profiles.add(msg.sender_user_id);
        }
        top.appendChild(sname_el);
        top.appendChild(make("span", "stream-time", fmt_relative(msg.created)));
        body.appendChild(top);

        var preview = make("div", "stream-preview");
        if (msg.text || msg.image) {
            var txt = (msg.image && !msg.text) ? "📷 Image" : msg.text;
            preview.appendChild(document.createTextNode(txt));
        } else if (msg.id) {
            preview.appendChild(document.createTextNode("Loading…"));
            request("fetch_remote_message", {
                message_id:   msg.id,
                peer_id:      msg.sender_peer_id || msg.peer_id || null,
                peer_address: msg.peer_address || "",
            }).then(function (d) {
                preview.replaceChildren();
                if (d.ok && d.message && (d.message.text || d.message.image)) {
                    var m = d.message;
                    preview.appendChild(document.createTextNode(
                        (m.image && !m.text) ? "📷 Image" : m.text
                    ));
                    msg.text  = m.text;
                    msg.image = m.image;
                } else {
                    preview.classList.add("unavailable");
                    preview.appendChild(document.createTextNode("Message unavailable — source offline"));
                }
            });
        }
        body.appendChild(preview);

        row.appendChild(body);
        div.appendChild(row);
        div.addEventListener("click", function () { open_channel(msg.channel_id, msg.peer_id || null, msg.id); });
        return div;
    }

    function build_stream_remote(ch, peer) {
        var div = make("div", "stream-item");
        var icon = make("div", "stream-icon", _channel_icon(ch));
        div.appendChild(icon);

        var body = make("div", "stream-body");
        var top  = make("div", "stream-top");
        var name = make("span", "stream-name", ch.name);
        top.appendChild(name);
        top.appendChild(make("span", "stream-peer", " @ " + (peer.name || peer.address)));
        if (ch.last_activity) top.appendChild(make("span", "stream-time", fmt_relative(ch.last_activity)));
        body.appendChild(top);

        var preview = make("div", "stream-preview");
        if (ch.last_sender_name) preview.appendChild(make("span", "preview-sender", ch.last_sender_name + ": "));
        preview.appendChild(document.createTextNode(ch.last_activity ? "…" : "No messages yet"));
        body.appendChild(preview);

        div.appendChild(body);
        div.addEventListener("click", function () { open_channel(ch.id, peer.id); });
        return div;
    }

    // ── Channel / chat ─────────────────────────────────────────────────
    function open_channel(channel_id, peer_id, scroll_to_id) {
        var active = document.querySelector(".nav-item.active");
        _chat_origin = (active && active.dataset.page) || "stream";
        current_channel_id   = channel_id;
        current_channel_peer = peer_id || null;
        pending_parent_id    = null;
        pending_image        = null;
        pending_scroll_id    = scroll_to_id || null;

        send("unsubscribe_all");
        document.body.classList.add("in-chat");

        show_page("/chat.html", function () {
            setup_chat_page();
            load_channel();
        });
    }

    function _upload_image_file(file) {
        if (!file) return;
        var form = new FormData();
        form.append("image", file);
        fetch(_bp + "/upload", { method: "POST", body: form })
            .then(function (r) {
                if (r.status === 401) { handle_auth_error(); return null; }
                return r.json();
            })
            .then(function (d) {
                if (!d) return;
                pending_image = d.image;
                var preview = document.getElementById("image_preview");
                preview.classList.remove("hidden");
                preview.replaceChildren();
                var img = document.createElement("img");
                img.src = _bp + "/img/" + d.image;
                preview.appendChild(img);
            })
            .catch(function () { show_error("Image upload failed"); });
    }

    function setup_chat_page() {
        document.getElementById("back_btn").addEventListener("click", function () {
            var dest = _chat_origin || "stream";
            exit_chat();
            set_active_nav(dest);
            show_page(_page_map[dest] || "/stream.html");
        });

        document.getElementById("drop_zone").addEventListener("click", function () {
            document.getElementById("image_file_input").click();
        });
        document.getElementById("image_file_input").addEventListener("change", function () {
            _upload_image_file(this.files[0]);
            this.value = "";
        });

        document.getElementById("send_btn").addEventListener("click", send_message);
        document.getElementById("message_input").addEventListener("keydown", function (e) {
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                send_message();
            }
        });
    }

function load_channel() {
        request("read_channel", {
            id:      current_channel_id,
            peer_id: current_channel_peer,
        }).then(function (d) {
            if (!d.ok) { show_error(d.reason); return; }

            var chan_name = (d.channel && d.channel.name) || "";
            var title = document.getElementById("chat_title");
            if (title) title.textContent = chan_name;
            if (chan_name) document.title = chan_name;

            var ch = d.channel || {};
            var icon_el = document.getElementById("chat_icon");
            if (icon_el) {
                icon_el.textContent = _channel_icon(ch);
                var can_edit = is_owner || (ch.created_by && String(ch.created_by) === String(user_id));
                if (can_edit) {
                    icon_el.title = "Change icon";
                    icon_el.style.cursor = "pointer";
                    icon_el.addEventListener("click", function (e) {
                        e.stopPropagation();
                        if (document.getElementById("icon_picker")) return;
                        var wrap = document.createElement("div");
                        wrap.id = "icon_picker";
                        wrap.className = "icon-picker-wrap";
                        icon_el.appendChild(wrap);

                        function _mount_picker(EmojiPickerEl) {
                            var picker = new EmojiPickerEl();
                            picker.dataSource = _bp + "/emoji-data.json";
                            wrap.appendChild(picker);
                            wrap.addEventListener("emoji-click", function (ev) {
                                var em = ev.detail.unicode;
                                if (!em) return;
                                request("set_channel_icon", { channel_id: ch.id, icon: em }).then(function (r) {
                                    if (!r.ok) { show_error(r.reason); return; }
                                    ch.icon = em;
                                    icon_el.textContent = em;
                                    wrap.remove();
                                });
                            });
                        }

                        if (window._EmojiPicker) {
                            _mount_picker(window._EmojiPicker);
                        } else {
                            import(_bp + "/emoji-picker.js").then(function (mod) {
                                window._EmojiPicker = mod.Picker;
                                _mount_picker(mod.Picker);
                            });
                        }

                        var close_picker = function () { wrap.remove(); };
                        setTimeout(function () {
                            document.addEventListener("click", close_picker, { once: true });
                        }, 0);
                    });
                }
            }

            var container = document.getElementById("messages");
            container.replaceChildren();
            d.messages.forEach(function (msg) { append_message(container, msg); });

            if (pending_scroll_id) {
                var target = container.querySelector("[data-id='" + pending_scroll_id + "']");
                if (target) {
                    target.scrollIntoView({ block: "center" });
                    target.classList.add("highlight");
                    setTimeout(function () { target.classList.remove("highlight"); }, 1500);
                } else {
                    container.scrollTop = container.scrollHeight;
                }
                pending_scroll_id = null;
            } else {
                container.scrollTop = container.scrollHeight;
            }

            request_profiles();

            if (!d.remote) {
                setup_chat_buttons(d.channel);
            }
        });
    }

    function setup_chat_buttons(channel) {
        var is_pub = channel && channel.public;

        // Members button (private channels only)
        var btn = document.getElementById("members_btn");
        if (btn && !is_pub) {
            btn.classList.remove("hidden");
            btn.addEventListener("click", function () {
                request("read_members", { channel_id: current_channel_id }).then(function (d) {
                    if (!d.ok) { show_error(d.reason); return; }
                    // Reset drop handlers to member mode
                    document.getElementById("members_list").setAttribute(
                        "ondrop", "communicatie.drop_member(event, true)");
                    document.getElementById("non_members_list").setAttribute(
                        "ondrop", "communicatie.drop_member(event, false)");
                    open_member_manager(d.members, d.non_members);
                });
            });
        }

        // Ban users button (public channels only)
        var bbtn = document.getElementById("bans_btn");
        if (bbtn && is_pub) {
            bbtn.classList.remove("hidden");
            bbtn.addEventListener("click", function () {
                request("read_bans", { channel_id: current_channel_id }).then(function (d) {
                    if (!d.ok) { show_error(d.reason); return; }
                    open_ban_manager(d.banned, d.not_banned);
                });
            });
        }

        document.getElementById("close_member_manager").addEventListener("click", function () {
            document.getElementById("member_manager").classList.add("hidden");
        });
    }

    // ── Messages ───────────────────────────────────────────────────────
    function reply_depth(container, parent_id, depth) {
        depth = depth || 1;
        var el = container.querySelector("[data-id='" + parent_id + "']");
        if (!el || !el.dataset.parentId) return depth;
        return reply_depth(container, el.dataset.parentId, depth + 1);
    }

    function append_message(container, msg) {
        if (_is_blocked(msg)) return;
        var is_reply = !!msg.parent_id;
        var div = make("div", "message" + (is_reply ? " reply" : ""));
        div.dataset.id = msg.id;
        if (is_reply) div.dataset.parentId = msg.parent_id;

        var img = avatar_img(
            msg.sender_avatar || (known_profiles[msg.sender_user_id] || {}).avatar
        );
        if (msg.sender_user_id && !known_profiles[msg.sender_user_id]) {
            needed_profiles.add(msg.sender_user_id);
        }

        var body   = make("div", "body");
        var header = make("div", "header");

        var is_remote   = !!msg.sender_peer_id;
        var sender_name = msg.sender_name
            || (known_profiles[msg.sender_user_id] || {}).name
            || (is_remote ? "@ " + (msg.peer_name || msg.peer_address || "?") : "…");

        var sender = make("span", "sender" + (is_remote ? " remote" : ""), sender_name);
        sender.dataset.uid = msg.sender_user_id;

        var time = make("span", "time", fmt_time(msg.created));
        header.appendChild(sender);
        header.appendChild(time);
        body.appendChild(header);

        var needs_fetch = msg.text === null && msg.image === null;
        if (needs_fetch) {
            body.appendChild(make("span", "unavailable", "Loading…"));
            fetch_remote_content(msg, body);
        } else {
            render_content(body, msg);
        }

        var actions = make("div", "actions");
        var reply   = make("button", null, "↩ Reply");
        reply.addEventListener("click", function () { start_reply(msg); });
        actions.appendChild(reply);
        body.appendChild(actions);

        div.appendChild(img);
        div.appendChild(body);

        if (is_reply) {
            var parent_el = container.querySelector("[data-id='" + msg.parent_id + "']");
            if (parent_el) {
                var depth = reply_depth(container, msg.parent_id);
                div.style.marginLeft = (depth * 2) + "rem";
                var after = parent_el;
                var next  = after.nextElementSibling;
                while (next && next.dataset.parentId === msg.parent_id) {
                    after = next;
                    next  = after.nextElementSibling;
                }
                after.insertAdjacentElement("afterend", div);
                return;
            }
        }
        container.appendChild(div);
    }

    function render_content(body, msg) {
        if (msg.text) body.appendChild(make("p", "text", msg.text));
        if (msg.image) {
            var img = document.createElement("img");
            img.className = "attach";
            img.src = (msg.image.startsWith("http") || msg.image.startsWith("/"))
                ? msg.image : _bp + "/img/" + msg.image;
            img.alt = "";
            img.addEventListener("click", function () { window.open(img.src); });
            body.appendChild(img);
        }
    }

    function fetch_remote_content(msg, body) {
        request("fetch_remote_message", {
            message_id:   msg.id,
            peer_id:      msg.sender_peer_id,
            peer_address: msg.peer_address || "",
        }).then(function (d) {
            body.querySelector(".unavailable")?.remove();
            if (d.ok && d.message) {
                render_content(body, d.message);
            } else {
                body.appendChild(make("span", "unavailable", "Message unavailable"));
            }
        });
    }

    function start_reply(msg) {
        pending_parent_id = msg.id;
        var ctx = document.getElementById("reply_context");
        ctx.classList.remove("hidden");
        ctx.replaceChildren();
        var label = make("span", null,
            "Replying to " + (msg.sender_name || "…") + ": " +
            (msg.text || "").slice(0, 40));
        var cancel = make("button", null, "✕");
        cancel.addEventListener("click", function () {
            pending_parent_id = null;
            ctx.classList.add("hidden");
        });
        ctx.appendChild(label);
        ctx.appendChild(cancel);
        document.getElementById("message_input").focus();
    }

    function send_message() {
        var text = document.getElementById("message_input").value.trim();
        if (!text && !pending_image) return;

        request("message", {
            channel_id: current_channel_id,
            peer_id:    current_channel_peer,
            text:       text || null,
            image:      pending_image || null,
            parent_id:  pending_parent_id || null,
        }).then(function (d) {
            if (!d.ok) show_error(d.reason || "Could not send message");
        });

        document.getElementById("message_input").value = "";
        document.getElementById("reply_context").classList.add("hidden");
        document.getElementById("image_preview").classList.add("hidden");
        pending_parent_id = null;
        pending_image     = null;
    }

    function on_live_message(msg) {
        var container = document.getElementById("messages");
        if (!container) return;
        if (msg.channel_id !== current_channel_id) return;
        if (document.querySelector("[data-id='" + msg.id + "']")) return;
        append_message(container, msg);
        container.scrollTop = container.scrollHeight;
        request_profiles();
    }

    // ── Image drop ─────────────────────────────────────────────────────
    function drop_image(event) {
        event.preventDefault();
        document.getElementById("drop_zone").classList.remove("active");
        _upload_image_file(event.dataTransfer.files[0]);
    }

    // ── Member / Block manager ─────────────────────────────────────────
    function _setup_manager_filters(llist, rlist) {
        var fl = document.getElementById("filter_left");
        var fr = document.getElementById("filter_right");
        if (fl) {
            fl.value = "";
            fl.oninput = function () {
                var q = this.value.toLowerCase();
                llist.querySelectorAll("li").forEach(function (li) {
                    li.style.display = li.textContent.toLowerCase().includes(q) ? "" : "none";
                });
            };
        }
        if (fr) {
            fr.value = "";
            fr.oninput = function () {
                var q = this.value.toLowerCase();
                rlist.querySelectorAll("li").forEach(function (li) {
                    li.style.display = li.textContent.toLowerCase().includes(q) ? "" : "none";
                });
            };
        }
    }

    function open_member_manager(members, non_members) {
        document.getElementById("manager_title").textContent  = "Members";
        document.getElementById("col_left_title").textContent = "Members";
        document.getElementById("col_right_title").textContent = "Not members";
        var mgr = document.getElementById("member_manager");
        mgr.classList.remove("hidden");

        var mlist  = document.getElementById("members_list");
        var nmlist = document.getElementById("non_members_list");
        mlist.replaceChildren();
        nmlist.replaceChildren();

        members.forEach(function (u)     { mlist.appendChild(member_item(u)); });
        non_members.forEach(function (u) { nmlist.appendChild(member_item(u)); });
        _setup_manager_filters(mlist, nmlist);
    }

    function open_ban_manager(blocked, not_banned) {
        document.getElementById("manager_title").textContent  = "Ban users";
        document.getElementById("col_left_title").textContent = "Blocked";
        document.getElementById("col_right_title").textContent = "Users";
        // Rewire drop handlers for blocks
        document.getElementById("members_list").setAttribute(
            "ondrop", "communicatie.drop_ban(event, true)");
        document.getElementById("non_members_list").setAttribute(
            "ondrop", "communicatie.drop_ban(event, false)");
        var mgr = document.getElementById("member_manager");
        mgr.classList.remove("hidden");

        var blist  = document.getElementById("members_list");
        var ublist = document.getElementById("non_members_list");
        blist.replaceChildren();
        ublist.replaceChildren();

        blocked.forEach(function (u)     { blist.appendChild(member_item(u)); });
        not_banned.forEach(function (u) { ublist.appendChild(member_item(u)); });
        _setup_manager_filters(blist, ublist);
    }

    function member_item(user) {
        var li = make("li", "member-item");
        li.draggable      = true;
        li.dataset.uid    = user.id;
        li.dataset.peerId = user.peer_id || "";
        var av = user.avatar;
        if (av && av.startsWith("http")) av = _bp + "/proxy_img?url=" + encodeURIComponent(av);
        li.appendChild(avatar_img(av));
        var label = user.name || "?";
        if (user.peer_name) label += " @ " + user.peer_name;
        li.appendChild(make("span", null, label));
        li.addEventListener("dragstart", function (e) {
            e.dataTransfer.setData("uid",     user.id);
            e.dataTransfer.setData("peer_id", user.peer_id || "");
            e.dataTransfer.setData("name",    user.name || "");
            e.dataTransfer.setData("avatar",  user.avatar || "");
        });
        return li;
    }

    function drop_member(event, make_member) {
        event.preventDefault();
        var uid     = event.dataTransfer.getData("uid");
        var peer_id = event.dataTransfer.getData("peer_id") || null;
        var name    = event.dataTransfer.getData("name")    || null;
        var avatar  = event.dataTransfer.getData("avatar")  || null;
        if (!uid) return;

        request("set_member", {
            channel_id: current_channel_id,
            user_id:    uid,
            peer_id:    peer_id,
            name:       name,
            avatar:     avatar,
            is_member:  make_member,
        }).then(function (d) {
            if (!d.ok) { show_error(d.reason); return; }
            var item = document.querySelector("[data-uid='" + uid + "']");
            var dest = make_member
                ? document.getElementById("members_list")
                : document.getElementById("non_members_list");
            if (item && dest) dest.appendChild(item);
        });
    }

    function drop_ban(event, do_ban) {
        event.preventDefault();
        var uid     = event.dataTransfer.getData("uid");
        var peer_id = event.dataTransfer.getData("peer_id") || null;
        if (!uid) return;

        request("set_ban", {
            channel_id: current_channel_id,
            user_id:    uid,
            peer_id:    peer_id,
            blocked:    do_ban,
        }).then(function (d) {
            if (!d.ok) { show_error(d.reason); return; }
            var item = document.querySelector("[data-uid='" + uid + "']");
            var dest = do_ban
                ? document.getElementById("members_list")
                : document.getElementById("non_members_list");
            if (item && dest) dest.appendChild(item);
        });
    }

    function _fmt_bytes(b) {
        if (b >= 1e12) return (b / 1e12).toFixed(1) + " TB";
        if (b >= 1e9)  return (b / 1e9).toFixed(1)  + " GB";
        if (b >= 1e6)  return (b / 1e6).toFixed(1)  + " MB";
        if (b >= 1e3)  return (b / 1e3).toFixed(1)  + " KB";
        return b + " B";
    }

    // ── Server page ────────────────────────────────────────────────────
    function _render_cert_info(info) {
        var el      = document.getElementById("cert_info_display");
        if (!el) return;
        var renew   = document.getElementById("cert_renew_btn");
        var reload  = document.getElementById("cert_reload_btn");
        var note    = document.getElementById("cert_external_note");
        if (!info || !info.cn) {
            el.textContent = "";
            if (renew)  renew.classList.add("hidden");
            if (reload) reload.classList.add("hidden");
            if (note)   note.classList.add("hidden");
            return;
        }
        el.replaceChildren();
        var badge = make("span",
            "cert-badge " + (info.self_signed ? "cert-self-signed" : "cert-valid"),
            info.self_signed ? "Self-signed" : "Valid certificate");
        el.appendChild(badge);
        var expiring = _cert_expiring_soon(info.expires, info.valid_days);
        var date_span = make("span", expiring ? "cert-detail cert-expiring" : "cert-detail",
            " — " + info.cn + " · valid until " + info.expires);
        el.appendChild(date_span);
        if (renew) {
            renew.classList.toggle("hidden", !info.self_signed);
            renew.title = expiring ? "Certificate expires soon — renew now" : "";
        }
        if (reload) reload.classList.toggle("hidden", !!info.self_signed);
        if (note)   note.classList.toggle("hidden",   !!info.self_signed);
    }

    function _db_toggle_fields(type) {
        document.getElementById("db_sqlite_fields").classList.toggle("hidden", type !== "sqlite");
        document.getElementById("db_pg_fields").classList.toggle("hidden",     type !== "postgres");
    }

    function _peer_last_seen_label(ts) {
        if (!ts) return "never seen";
        var diff = (new Date() - new Date(ts)) / 1000;
        if (diff < 60)   return "just now";
        if (diff < 3600) return Math.floor(diff / 60) + "m ago";
        if (diff < 86400) return Math.floor(diff / 3600) + "h ago";
        return Math.floor(diff / 86400) + "d ago";
    }

    function build_peer_item(p, plist) {
        var li  = make("li");
        var st  = p.session_status || (p.connected ? "connected" : "disconnected");
        li.dataset.name   = ((p.name || "") + " " + (p.address || "")).toLowerCase();
        li.dataset.dbst   = p.status || "approved";
        li.dataset.sessst = st;
        var dot_cls = {
            connected:    "peer-online",
            idle:         "peer-idle",
            offline:      "peer-was-connected",
            awaiting:     "peer-awaiting",
            failed:       "peer-failed",
            disconnected: "peer-offline",
        }[st] || "peer-offline";
        var dot = make("span", "peer-dot " + dot_cls);
        var seen = _peer_last_seen_label(p.last_seen);
        var reason_str = (st === "failed" && p.session_reason) ? ": " + p.session_reason : "";
        dot.title = st + reason_str + " · last seen " + seen;
        li.appendChild(dot);
        var label = make("span", null, p.name || p.address);
        if (p.name) label.title = p.address;
        li.appendChild(label);

        if (p.status === "pending") li.appendChild(make("span", "peer-badge badge-pending", "pending"));
        if (p.status === "blocked") li.appendChild(make("span", "peer-badge badge-blocked", "blocked"));
        if (st === "awaiting")
            li.appendChild(make("span", "peer-badge badge-pending", "Awaiting approval"));
        if (st === "failed" && p.session_reason)
            li.appendChild(make("span", "peer-badge badge-blocked", p.session_reason));

        if (is_owner) {
            if (p.status === "pending") {
                var ok_btn = make("button", "btn-sm", "✓ Approve");
                ok_btn.addEventListener("click", function () {
                    request("approve_peer", { id: p.id, status: "approved" }).then(function (r) {
                        if (!r.ok) { show_error(r.reason); return; }
                        p.status = "approved"; li.replaceWith(build_peer_item(p, plist));
                        _check_nav_badges();
                    });
                });
                var no_btn = make("button", "btn-sm", "✗ Block");
                no_btn.addEventListener("click", function () {
                    request("approve_peer", { id: p.id, status: "blocked" }).then(function (r) {
                        if (!r.ok) { show_error(r.reason); return; }
                        p.status = "blocked"; li.replaceWith(build_peer_item(p, plist));
                        _check_nav_badges();
                    });
                });
                li.appendChild(ok_btn);
                li.appendChild(no_btn);
            } else if (p.status === "blocked") {
                var unblock_btn = make("button", "btn-sm", "↩");
                unblock_btn.title = "Unblock";
                unblock_btn.addEventListener("click", function () {
                    request("approve_peer", { id: p.id, status: "approved" }).then(function (r) {
                        if (!r.ok) { show_error(r.reason); return; }
                        p.status = "approved"; li.replaceWith(build_peer_item(p, plist));
                    });
                });
                li.appendChild(unblock_btn);
            }

            var trash = make("span", "trash", "✕");
            trash.title = p.status === "blocked" ? "Permanently remove" : "Block";
            trash.addEventListener("click", function () {
                var msg = p.status === "blocked"
                    ? "Permanently remove " + (p.name || p.address) + "?"
                    : "Block " + (p.name || p.address) + "? They will no longer be able to connect.";
                if (!confirm(msg)) return;
                request("delete_peer", { id: p.id }).then(function (r) {
                    if (!r.ok) { show_error(r.reason); return; }
                    if (r.removed) {
                        li.remove();
                    } else {
                        p.status = "blocked";
                        li.replaceWith(build_peer_item(p, plist));
                        _check_nav_badges();
                    }
                });
            });
            li.appendChild(trash);
        }
        return li;
    }

    function _setup_peer_filters(plist, peers) {
        var name_input   = document.getElementById("peer_filter_name");
        var chips_el     = document.getElementById("peer_filter_status");
        if (!name_input || !chips_el) return;

        // Collect statuses present in list
        var status_labels = {
            connected: "Connected", idle: "Idle", offline: "Offline",
            awaiting: "Awaiting", failed: "Failed", disconnected: "Disconnected",
            pending: "Pending", blocked: "Blocked",
        };
        var present = {};
        peers.forEach(function (p) {
            var st = p.session_status || (p.connected ? "connected" : "disconnected");
            present[st] = true;
            if (p.status && p.status !== "approved") present[p.status] = true;
        });

        var active_filters = {};  // status key → true if filter active

        chips_el.replaceChildren();
        Object.keys(present).forEach(function (key) {
            var chip = make("span", "peer-chip", status_labels[key] || key);
            chip.dataset.key = key;
            chip.addEventListener("click", function () {
                if (active_filters[key]) {
                    delete active_filters[key];
                    chip.classList.remove("peer-chip-active");
                } else {
                    active_filters[key] = true;
                    chip.classList.add("peer-chip-active");
                }
                _apply_peer_filter();
            });
            chips_el.appendChild(chip);
        });

        function _apply_peer_filter() {
            var q   = (name_input.value || "").toLowerCase();
            var any = Object.keys(active_filters).length > 0;
            plist.querySelectorAll("li").forEach(function (li) {
                var name_ok   = !q || li.dataset.name.includes(q);
                var status_ok = !any || active_filters[li.dataset.dbst] || active_filters[li.dataset.sessst];
                li.style.display = (name_ok && status_ok) ? "" : "none";
            });
        }

        name_input.value = "";
        name_input.oninput = _apply_peer_filter;
    }

    function setup_peers_page() {
        request("read_peers").then(function (d) {
            if (!d.ok) return;

            var plist = document.getElementById("peers_list");
            plist.replaceChildren();
            (d.peers || []).forEach(function (p) {
                plist.appendChild(build_peer_item(p, plist));
            });

            // Update badge immediately from loaded data
            if (is_owner) {
                var pending = (d.peers || []).filter(function (p) { return p.status === "pending"; }).length;
                set_nav_badge("peers", pending ? String(pending) : "");
            }

            // Filter bar
            _setup_peer_filters(plist, d.peers || []);

            if (!is_owner) return;

            // Policy
            var policy_sec = document.getElementById("peer_policy_section");
            policy_sec.classList.remove("hidden");
            policy_sec.querySelectorAll("input[name='peer_policy']").forEach(function (r) {
                r.checked = (r.value === (d.policy || "open"));
                r.addEventListener("change", function () {
                    request("set_peer_policy", { policy: r.value });
                });
            });

            document.getElementById("peer_admin").classList.remove("hidden");
            document.getElementById("add_peer_form").addEventListener("submit", function (e) {
                e.preventDefault();
                var addr = document.getElementById("peer_address_input").value.trim();
                if (!addr) return;
                request("add_peer", { address: addr }).then(function (r) {
                    if (!r.ok) { show_error(r.reason); return; }
                    document.getElementById("peer_address_input").value = "";
                    r.peer.status = r.peer.status || "approved";
                    if (r.pending) r.peer.session_status = "awaiting";
                    plist.appendChild(build_peer_item(r.peer, plist));
                    if (r.pending) show_error("Connection pending approval on the remote server");
                });
            });
        });
    }

    function setup_advanced_page() {
        document.querySelectorAll(".info-btn").forEach(function (btn) {
            btn.addEventListener("click", function (e) {
                e.stopPropagation();
                var open = btn.classList.contains("open");
                document.querySelectorAll(".info-btn.open").forEach(function (b) { b.classList.remove("open"); });
                if (!open) btn.classList.add("open");
            });
        });

        if (!is_owner) return;

        request("read_advanced_config").then(function (d) {
            if (!d.ok) return;

            document.getElementById("adv_base_path").value      = d.base_path || "";
            document.getElementById("adv_host").value           = d.host || "";
            document.getElementById("adv_port").value           = d.port || "";
            document.getElementById("adv_port_http").value      = d.port_http || 0;
            document.getElementById("adv_rate_login").value     = d.rate_limit_login;
            document.getElementById("adv_rate_messages").value  = d.rate_limit_messages;
            document.getElementById("adv_trusted_proxies").value = d.trusted_proxies || "";
            document.getElementById("adv_peer_timeout").value   = d.peer_connect_timeout;
            document.getElementById("adv_session_days").value   = d.session_max_age_days;
            document.getElementById("adv_upload_mb").value      = d.upload_max_mb;
        });

        function _save(ids, show_ok_id, show_restart_id) {
            var payload = {};
            ids.forEach(function (id) {
                var el = document.getElementById(id);
                if (el && !el.readOnly) payload[id.replace("adv_", "")] = el.value;
            });
            request("set_advanced_config", payload).then(function (r) {
                if (!r.ok) { show_error(r.reason); return; }
                if (r.needs_restart && show_restart_id)
                    document.getElementById(show_restart_id).classList.remove("hidden");
                if (!r.needs_restart && show_ok_id)
                    document.getElementById(show_ok_id).classList.remove("hidden");
            });
        }

        document.getElementById("adv_network_save").addEventListener("click", function () {
            _save(["adv_host", "adv_port", "adv_port_http"], null, "adv_network_restart");
        });
        document.getElementById("adv_ratelimit_save").addEventListener("click", function () {
            _save(["adv_rate_login", "adv_rate_messages", "adv_trusted_proxies"], "adv_ratelimit_ok", null);
        });
        document.getElementById("adv_federation_save").addEventListener("click", function () {
            _save(["adv_peer_timeout"], "adv_federation_ok", null);
        });
        document.getElementById("adv_sessions_save").addEventListener("click", function () {
            _save(["adv_session_days"], null, "adv_sessions_restart");
        });
        document.getElementById("adv_uploads_save").addEventListener("click", function () {
            _save(["adv_upload_mb"], null, "adv_uploads_restart");
        });

        // ── Updates ────────────────────────────────────────────────────────────
        request("read_update_config").then(function (d) {
            if (!d.ok) return;
            var verEl = document.getElementById("adv_version_line");
            if (verEl) verEl.textContent = "Current version: " + d.current_version;
            var cb = document.getElementById("adv_auto_update");
            if (cb) cb.checked = !!d.auto_update;
        });

        document.getElementById("adv_auto_update").addEventListener("change", function () {
            request("set_update_config", { auto_update: this.checked }).then(function (r) {
                if (!r.ok) show_error(r.reason);
            });
        });

        document.getElementById("adv_check_update_btn").addEventListener("click", function () {
            var statusEl = document.getElementById("adv_update_status");
            var applyBtn = document.getElementById("adv_apply_update_btn");
            statusEl.textContent = "Checking…";
            applyBtn.classList.add("hidden");
            request("check_update").then(function (d) {
                if (!d.ok) { statusEl.textContent = "Error: " + d.reason; return; }
                if (d.update_available) {
                    statusEl.textContent = "Update available: " + d.current + " → " + d.latest;
                    applyBtn.classList.remove("hidden");
                } else if (d.latest) {
                    statusEl.textContent = "Up to date (" + d.current + ")";
                } else {
                    statusEl.textContent = d.message || "No releases found.";
                }
            });
        });

        document.getElementById("adv_apply_update_btn").addEventListener("click", function () {
            var statusEl = document.getElementById("adv_update_status");
            var self = this;
            self.disabled = true;
            statusEl.textContent = "Applying update…";
            request("apply_update").then(function (d) {
                if (!d.ok) {
                    statusEl.textContent = "Error: " + d.reason;
                    self.disabled = false;
                    return;
                }
                statusEl.textContent = d.message + " Reconnecting…";
                self.classList.add("hidden");
            });
        });
    }

    function setup_server_page() {
        document.querySelectorAll(".info-btn").forEach(function (btn) {
            btn.addEventListener("click", function (e) {
                e.stopPropagation();
                var open = btn.classList.contains("open");
                document.querySelectorAll(".info-btn.open").forEach(function (b) { b.classList.remove("open"); });
                if (!open) btn.classList.add("open");
            });
        });

        var adv_link = document.getElementById("advanced_link");
        if (adv_link) {
            if (!is_owner) { adv_link.style.display = "none"; }
            else adv_link.addEventListener("click", function (e) {
                e.preventDefault();
                show_page("/advanced.html");
            });
        }

        function refresh_identity(d) {
            if (!d || !d.ok) return;
            var field = document.getElementById("peer_address_field");
            if (field && d.peer_address) field.value = d.peer_address;
            if (d.peer_name) {
                var ni = document.getElementById("peer_name_input");
                if (ni) ni.value = d.peer_name;
            }
        }

        request("read_peers").then(function (d) {
            refresh_identity(d);

            var field      = document.getElementById("peer_address_field");
            var copy_btn   = document.getElementById("peer_address_copy_btn");
            var edit_btn   = document.getElementById("peer_address_edit_btn");
            var save_btn   = document.getElementById("peer_address_save_btn");
            var cancel_btn = document.getElementById("peer_address_cancel_btn");

            copy_btn.addEventListener("click", function () {
                navigator.clipboard.writeText(field.value).then(function () {
                    copy_btn.textContent = "Copied!";
                    setTimeout(function () { copy_btn.textContent = "Copy"; }, 2000);
                });
            });

            if (!is_owner) return;

            var name_input = document.getElementById("peer_name_input");
            if (name_input) name_input.removeAttribute("readonly");
            var name_save = document.getElementById("peer_name_save_btn");
            if (name_save) name_save.classList.remove("hidden");

            edit_btn.classList.remove("hidden");

            function enter_edit() {
                field.removeAttribute("readonly");
                field.focus();
                edit_btn.classList.add("hidden");
                copy_btn.classList.add("hidden");
                save_btn.classList.remove("hidden");
                cancel_btn.classList.remove("hidden");
            }
            function exit_edit(restore_val) {
                if (restore_val !== undefined) field.value = restore_val;
                field.setAttribute("readonly", "");
                edit_btn.classList.remove("hidden");
                copy_btn.classList.remove("hidden");
                save_btn.classList.add("hidden");
                cancel_btn.classList.add("hidden");
            }

            edit_btn.addEventListener("click", function () {
                enter_edit();
            });
            cancel_btn.addEventListener("click", function () {
                exit_edit(d.peer_address || "");
            });
            save_btn.addEventListener("click", function () {
                var addr = field.value.trim();
                if (!addr) return;
                request("set_peer_address", { address: addr }).then(function (r) {
                    if (!r.ok) { show_error(r.reason); return; }
                    d.peer_address = addr;
                    exit_edit();
                });
            });

            document.getElementById("peer_name_form").addEventListener("submit", function (e) {
                e.preventDefault();
                var name = document.getElementById("peer_name_input").value.trim();
                if (!name) return;
                request("set_peer_name", { name: name }).then(function (r) {
                    if (!r.ok) show_error(r.reason);
                });
            });

            // Upload dir (owner only)
            request("read_upload_config").then(function (uc) {
                if (!uc.ok) return;
                var section    = document.getElementById("upload_section");
                var field      = document.getElementById("upload_dir_field");
                var edit_btn   = document.getElementById("upload_dir_edit_btn");
                var save_btn   = document.getElementById("upload_dir_save_btn");
                var cancel_btn = document.getElementById("upload_dir_cancel_btn");
                var note       = document.getElementById("upload_dir_restart_note");
                section.classList.remove("hidden");
                field.value = uc.upload_dir || "";
                edit_btn.classList.remove("hidden");
                if (uc.disk) {
                    document.getElementById("upload_dir_disk_info").textContent =
                        _fmt_bytes(uc.disk.free) + " free of " + _fmt_bytes(uc.disk.total);
                }

                var migrate_opts = document.getElementById("upload_migrate_options");

                function enter_edit() {
                    field.removeAttribute("readonly"); field.focus();
                    edit_btn.classList.add("hidden");
                    save_btn.classList.remove("hidden");
                    cancel_btn.classList.remove("hidden");
                    migrate_opts.classList.remove("hidden");
                    // reset to "none"
                    migrate_opts.querySelector("input[value='none']").checked = true;
                }
                function exit_edit(restore) {
                    if (restore !== undefined) field.value = restore;
                    field.setAttribute("readonly", "");
                    edit_btn.classList.remove("hidden");
                    save_btn.classList.add("hidden");
                    cancel_btn.classList.add("hidden");
                    migrate_opts.classList.add("hidden");
                }
                edit_btn.addEventListener("click", enter_edit);
                cancel_btn.addEventListener("click", function () { exit_edit(uc.upload_dir || ""); });
                save_btn.addEventListener("click", function () {
                    var val     = field.value.trim();
                    var migrate = migrate_opts.querySelector("input[name='upload_migrate']:checked").value;
                    if (!val) return;
                    save_btn.disabled = true;
                    request("set_upload_config", { upload_dir: val, migrate: migrate }).then(function (r) {
                        save_btn.disabled = false;
                        if (!r.ok) { show_error(r.reason); return; }
                        uc.upload_dir = val;
                        exit_edit();
                        note.classList.remove("hidden");
                    });
                });
            });

            // Database config (owner only)
            request("read_db_config").then(function (dc) {
                if (!dc.ok) return;
                var section = document.getElementById("db_section");
                section.classList.remove("hidden");

                var radios = section.querySelectorAll("input[name='db_type']");
                radios.forEach(function (r) { r.checked = (r.value === dc.db_type); });
                if (dc.db_type === "sqlite") {
                    document.getElementById("db_sqlite_path").value = dc.sqlite_path || "";
                } else {
                    document.getElementById("db_pg_dsn").value = dc.dsn || "";
                }
                _db_toggle_fields(dc.db_type);
                radios.forEach(function (r) {
                    r.addEventListener("change", function () { _db_toggle_fields(r.value); });
                });
                document.getElementById("db_form").addEventListener("submit", function (e) {
                    e.preventDefault();
                    var type = section.querySelector("input[name='db_type']:checked").value;
                    var params = { db_type: type };
                    if (type === "sqlite") {
                        params.path = document.getElementById("db_sqlite_path").value.trim();
                    } else {
                        params.dsn = document.getElementById("db_pg_dsn").value.trim();
                    }
                    request("set_db_config", params).then(function (r) {
                        if (!r.ok) { show_error(r.reason); return; }
                        document.getElementById("db_restart_note").classList.remove("hidden");
                    });
                });
            });

            // Certificate config (owner only)
            request("read_cert_config").then(function (cc) {
                if (!cc.ok) return;
                var section = document.getElementById("cert_section");
                section.classList.remove("hidden");
                document.getElementById("cert_path_input").value = cc.cert_path || "";
                document.getElementById("cert_key_input").value  = cc.key_path  || "";
                _render_cert_info(cc.info);

                document.getElementById("cert_renew_btn").addEventListener("click", function () {
                    if (!confirm("Renew the current self-signed certificate? Existing peers will update automatically via succession.")) return;
                    this.disabled = true;
                    var btn = this;
                    request("renew_cert").then(function (r) {
                        btn.disabled = false;
                        if (!r.ok) { show_error(r.reason); return; }
                        _render_cert_info(r.info);
                        _check_nav_badges();
                    });
                });

                document.getElementById("cert_reload_btn").addEventListener("click", function () {
                    this.disabled = true;
                    var btn = this;
                    request("reload_cert").then(function (r) {
                        btn.disabled = false;
                        if (!r.ok) { show_error(r.reason); return; }
                        _render_cert_info(r.info);
                        _check_nav_badges();
                    });
                });

                document.getElementById("cert_form").addEventListener("submit", function (e) {
                    e.preventDefault();
                    request("set_cert_config", {
                        cert_path: document.getElementById("cert_path_input").value.trim(),
                        key_path:  document.getElementById("cert_key_input").value.trim(),
                    }).then(function (r) {
                        if (!r.ok) { show_error(r.reason); return; }
                        _render_cert_info(r.info);
                        document.getElementById("cert_restart_note").classList.remove("hidden");
                        if (r.domain_warning) show_error("⚠ " + r.domain_warning);
                    });
                });
            });
        });
    }

    // ── Users page ─────────────────────────────────────────────────────
    // ── Collapsible peer sections + filter ────────────────────────────
    function _setup_collapsible_filter(list, filter_input, rowSel) {
        rowSel = rowSel || "li";
        // Make peer-header rows collapsible
        list.querySelectorAll(".peer-header").forEach(function (hdr) {
            hdr.style.cursor = "pointer";
            hdr.dataset.collapsed = "1";   // collapsed by default
            var arrow = make("span", "peer-collapse-arrow", "▸");
            hdr.insertBefore(arrow, hdr.firstChild);
            hdr.addEventListener("click", function () {
                var collapsed = hdr.dataset.collapsed === "1";
                hdr.dataset.collapsed = collapsed ? "" : "1";
                arrow.textContent = collapsed ? "▾" : "▸";
                _apply_filter();
            });
        });

        function _items_for_header(hdr) {
            var items = [];
            var el = hdr.nextElementSibling;
            while (el && !el.classList.contains("peer-header")) {
                items.push(el);
                el = el.nextElementSibling;
            }
            return items;
        }

        function _apply_filter() {
            var q = filter_input ? filter_input.value.toLowerCase() : "";
            list.querySelectorAll(".peer-header").forEach(function (hdr) {
                var collapsed = hdr.dataset.collapsed === "1";
                var items     = _items_for_header(hdr);
                var any_match = false;
                items.forEach(function (row) {
                    var text  = row.textContent.toLowerCase();
                    var match = !q || text.includes(q);
                    row.style.display = (match && (q || !collapsed)) ? "" : "none";
                    if (match) any_match = true;
                });
                hdr.style.display = (!q || any_match) ? "" : "none";
            });
            // Local (non-peer-header) items
            list.querySelectorAll(rowSel + ":not(.peer-header)").forEach(function (row) {
                var prev = row.previousElementSibling;
                while (prev && !prev.classList.contains("peer-header")) {
                    prev = prev.previousElementSibling;
                }
                if (prev) return;  // handled by peer-header loop above
                var text = row.textContent.toLowerCase();
                row.style.display = (!q || text.includes(q)) ? "" : "none";
            });
        }

        if (filter_input) {
            filter_input.value = "";
            filter_input.oninput = _apply_filter;
        }
        _apply_filter();
    }

    function _build_user_item(u) {
        var tr      = document.createElement("tr");
        var is_self = u.id === user_id && !u.peer_id;

        // ── Actions cell ──
        var td_acts = document.createElement("td");
        td_acts.className = "td-acts";
        var acts = make("div", "user-actions");

        var dm_btn = make("button", "btn-sm", is_self ? "🗒" : "💬");
        dm_btn.title = is_self ? "Scratchpad" : "Direct message";
        dm_btn.addEventListener("click", function () {
            request("start_chat", { user_id: u.id, peer_id: u.peer_id || null,
                                    name: u.name || null, avatar: u.avatar || null })
                .then(function (r) {
                    if (!r.ok) { show_error(r.reason); return; }
                    open_channel(r.channel_id, null, null);
                });
        });
        acts.appendChild(dm_btn);

        if (!is_self) {
            var bkey = _block_key(u.id, u.peer_id || null);
            var block_btn = make("button", "btn-sm", _blocked_users.has(bkey) ? "🚫" : "🔕");
            block_btn.title = _blocked_users.has(bkey) ? "Unblock user" : "Block user";
            block_btn.addEventListener("click", function () {
                if (_blocked_users.has(bkey)) {
                    _blocked_users.delete(bkey);
                    block_btn.textContent = "🔕";
                    block_btn.title = "Block user";
                } else {
                    _blocked_users.add(bkey);
                    block_btn.textContent = "🚫";
                    block_btn.title = "Unblock user";
                }
                _save_blocked_users();
            });
            acts.appendChild(block_btn);
        }

        if (is_owner && !u.peer_id) {
            var trash = make("span", "trash", "✕");
            trash.addEventListener("click", function () {
                if (!confirm("Delete " + u.name + "?")) return;
                request("delete_user", { id: u.id }).then(function (r) {
                    if (r.ok) tr.remove();
                    else show_error(r.reason);
                });
            });
            acts.appendChild(trash);
        }

        td_acts.appendChild(acts);

        // ── Identity cell ──
        var td_ident = document.createElement("td");
        td_ident.className = "user-identity";
        td_ident.style.cursor = "pointer";
        td_ident.appendChild(avatar_img(u.avatar));
        td_ident.appendChild(make("span", null, u.name));
        td_ident.addEventListener("click", function () { open_user_profile(u); });

        tr.appendChild(td_ident);
        tr.appendChild(td_acts);
        return tr;
    }

    function open_user_profile(u) {
        var is_self = u.id === user_id && !u.peer_id;
        _profile_target = is_self ? null : u;
        set_active_nav("user");
        show_page("/user.html");
    }

    function setup_users_page() {
        request("read_users").then(function (d) {
            if (!d.ok) return;
            var tbody = document.getElementById("users_list");
            tbody.replaceChildren();
            d.users.forEach(function (u) { tbody.appendChild(_build_user_item(u)); });

            // Remote users grouped by peer
            if (d.remote_users && d.remote_users.length) {
                var by_peer = {};
                d.remote_users.forEach(function (u) {
                    var key = u.peer_name || u.peer_id || "?";
                    (by_peer[key] = by_peer[key] || []).push(u);
                });
                Object.keys(by_peer).sort().forEach(function (peer_name) {
                    var hdr_tr = document.createElement("tr");
                    hdr_tr.className = "peer-header";
                    var hdr_td = document.createElement("td");
                    hdr_td.colSpan = 2;
                    hdr_td.textContent = peer_name;
                    hdr_tr.appendChild(hdr_td);
                    tbody.appendChild(hdr_tr);
                    by_peer[peer_name].forEach(function (u) {
                        tbody.appendChild(_build_user_item(u));
                    });
                });
            }

            _setup_collapsible_filter(tbody, document.getElementById("users_filter"), "tr");
        });
    }

    // ── Channels page ──────────────────────────────────────────────────
    function setup_channels_page() {
        request("read_channels").then(function (d) {
            if (!d.ok) return;

            var list = document.getElementById("channels_list");
            list.replaceChildren();
            d.channels.forEach(function (ch) {
                list.appendChild(build_channel_item(list, ch));
            });

            document.getElementById("local_filter").addEventListener("input", function () {
                var q = this.value.toLowerCase();
                list.querySelectorAll("li").forEach(function (li) {
                    var name = (li.querySelector("a") || {}).textContent || "";
                    li.style.display = name.toLowerCase().includes(q) ? "" : "none";
                });
            });

            var rlist = document.getElementById("remote_channels_list");
            var section = document.getElementById("remote_channels_section");
            if (d.peers && d.peers.some(function (p) { return p.channels && p.channels.length; })) {
                rlist.replaceChildren();
                d.peers.forEach(function (peer) {
                    if (!peer.channels || !peer.channels.length) return;
                    var header = make("li", "peer-header",
                        peer.name || peer.address);
                    rlist.appendChild(header);
                    peer.channels.forEach(function (ch) {
                        var li   = make("li", "peer-channel");
                        var icon = make("span", "icon", _channel_icon(ch));
                        var link = make("a", null, ch.name);
                        link.href = "#";
                        link.addEventListener("click", function (e) {
                            e.preventDefault();
                            open_channel(ch.id, peer.id);
                        });
                        li.appendChild(icon);
                        li.appendChild(link);

                        var mkey  = (ch.id || "") + "|" + (peer.id || "");
                        // public: opt-in (show if subscribed); private: opt-out (hide if muted)
                        var muted = ch.public ? !_stream_subscribed.has(mkey) : _stream_muted.has(mkey);
                        var mbtn  = make("span", "stream-toggle", muted ? "🔕" : "🔔");
                        mbtn.title = muted ? "Add to stream" : "Remove from stream";
                        mbtn.addEventListener("click", function (e) {
                            e.stopPropagation();
                            muted = !muted;
                            if (ch.public) {
                                if (muted) { _stream_subscribed.delete(mkey); }
                                else       { _stream_subscribed.add(mkey); }
                                _save_stream_subscribed();
                            } else {
                                if (muted) { _stream_muted.add(mkey); }
                                else       { _stream_muted.delete(mkey); }
                                _save_stream_muted();
                            }
                            mbtn.textContent = muted ? "🔕" : "🔔";
                            mbtn.title = muted ? "Add to stream" : "Remove from stream";
                        });
                        li.appendChild(mbtn);

                        rlist.appendChild(li);
                    });
                });
            } else {
                section.classList.add("hidden");
            }

            _setup_collapsible_filter(rlist, document.getElementById("remote_filter"));
        });

        document.getElementById("create_channel_form").addEventListener("submit", function (e) {
            e.preventDefault();
            var name    = document.getElementById("channel_name").value.trim();
            var private_ = document.getElementById("channel_private").checked;
            if (!name) return;
            request("create_channel", { name: name, public: !private_ }).then(function (d) {
                if (!d.ok) { show_error(d.reason); return; }
                document.getElementById("channel_name").value = "";
                document.getElementById("channel_private").checked = false;
                var list = document.getElementById("channels_list");
                list.appendChild(build_channel_item(list, d.channel));
            });
        });
    }

    function build_channel_item(list, ch) {
        var is_mine = ch.created_by === user_id;
        var li   = make("li", is_mine ? "own-channel" : "");
        var icon = make("span", "icon", _channel_icon(ch));
        var link = make("a", null, ch.name);
        link.href = "#";
        link.addEventListener("click", function (e) {
            e.preventDefault();
            open_channel(ch.id, null);
        });
        li.appendChild(icon);
        li.appendChild(link);
        var creator_label = is_mine ? "jij" : (ch.created_by_name || "");
        if (creator_label) li.appendChild(make("span", "channel-creator", creator_label));

        var excluded = !!ch.stream_excluded;
        var stream_btn = make("span", "stream-toggle", excluded ? "🔕" : "🔔");
        stream_btn.title = excluded ? "Add to stream" : "Remove from stream";
        stream_btn.addEventListener("click", function () {
            request("toggle_stream_channel", { channel_id: ch.id }).then(function (d) {
                if (!d.ok) { show_error(d.reason); return; }
                excluded = d.stream_excluded;
                stream_btn.textContent = excluded ? "🔕" : "🔔";
                stream_btn.title = excluded ? "Add to stream" : "Remove from stream";
            });
        });
        li.appendChild(stream_btn);

        if (is_owner || ch.created_by === user_id) {
            var trash = make("span", "trash", "✕");
            trash.addEventListener("click", function () {
                if (!confirm("Delete channel " + ch.name + "?")) return;
                request("delete_channel", { id: ch.id }).then(function (d) {
                    if (d.ok) li.remove();
                    else show_error(d.reason);
                });
            });
            li.appendChild(trash);
        }
        return li;
    }

    // ── Profile page ───────────────────────────────────────────────────
    function setup_user_page() {
        var target        = _profile_target;
        _profile_target   = null;
        var own_actions   = document.getElementById("own_actions");
        var admin_actions = document.getElementById("admin_actions");
        var avatar_form   = document.getElementById("avatar_form");
        var peer_el       = document.getElementById("user_peer");

        function load_user_messages(uid, pid) {
            request("read_user_messages", { user_id: uid, peer_id: pid || null })
                .then(function (d) {
                    if (!d.ok || !d.messages || !d.messages.length) return;
                    var section = document.getElementById("user_messages_section");
                    var list    = document.getElementById("user_messages_list");
                    section.classList.remove("hidden");
                    list.replaceChildren();
                    d.messages.forEach(function (m) {
                        var li  = make("li", "user-msg-item");
                        var top = make("div", "user-msg-top");
                        top.appendChild(make("span", "user-msg-channel",
                            (m.channel_public ? "🌐" : "🔒") + " " + m.channel_name));
                        top.appendChild(make("span", "stream-time", fmt_relative(m.created)));
                        li.appendChild(top);
                        var txt = m.image && !m.text ? "📷 Image" : (m.text || "…");
                        li.appendChild(make("p", "user-msg-text", txt));
                        li.style.cursor = "pointer";
                        li.addEventListener("click", function () {
                            open_channel(m.channel_id, pid || null, m.id);
                        });
                        list.appendChild(li);
                    });
                });
        }

        if (!target) {
            // ── Own profile ──
            document.getElementById("profile_title").textContent = "Profile";
            if (own_actions)  own_actions.classList.remove("hidden");
            if (avatar_form)  avatar_form.classList.remove("hidden");

            request("read_user", { id: user_id }).then(function (d) {
                if (!d.ok) return;
                var u = d.user;
                var display = u.display_name || u.name;
                document.getElementById("user_name_display").textContent = display;
                document.getElementById("user_joined").textContent =
                    "Joined " + new Date(u.created).toLocaleDateString();
                if (u.avatar) {
                    document.getElementById("user_avatar").src = _bp + "/img/" + u.avatar;
                    var nav_img = document.getElementById("nav-avatar");
                    if (nav_img) nav_img.src = _bp + "/img/" + u.avatar;
                }
                var dn_input = document.getElementById("display_name_input");
                if (dn_input) dn_input.value = u.display_name || "";
                var hint = document.getElementById("login_name_hint");
                if (hint) hint.textContent = "Login name: " + u.name;
            });

            document.getElementById("avatar_input").addEventListener("change", function () {
                var file = this.files[0];
                if (!file) return;
                var form = new FormData();
                form.append("avatar", file);
                fetch(_bp + "/set_avatar", { method: "POST", body: form })
                    .then(function (r) {
                        if (r.status === 401) { handle_auth_error(); return null; }
                        return r.json();
                    })
                    .then(function (d) {
                        if (!d) return;
                        document.getElementById("user_avatar").src = _bp + "/img/" + d.avatar;
                        var nav_img = document.getElementById("nav-avatar");
                        if (nav_img) nav_img.src = _bp + "/img/" + d.avatar;
                    })
                    .catch(function () { show_error("Avatar upload failed"); });
            });

            load_user_messages(user_id, null);

            document.getElementById("display_name_form").addEventListener("submit", function (e) {
                e.preventDefault();
                var val = document.getElementById("display_name_input").value.trim();
                request("set_display_name", { display_name: val }).then(function (r) {
                    if (!r.ok) { show_error(r.reason); return; }
                    var shown = r.name;
                    document.getElementById("user_name_display").textContent = shown;
                    user_name = shown;
                    known_profiles[user_id] = Object.assign(known_profiles[user_id] || {}, { name: shown });
                });
            });

            document.getElementById("logout_btn").addEventListener("click", function () {
                location.href = _bp + "/logout";
            });
            document.getElementById("delete_account_btn").addEventListener("click", function () {
                if (!confirm("Delete your account? This cannot be undone.")) return;
                request("delete_user", { id: user_id }).then(function (d) {
                    if (d.ok) location.href = _bp + "/logout";
                    else show_error(d.reason);
                });
            });

        } else {
            // ── Another user's profile ──
            document.getElementById("profile_title").textContent =
                target.peer_name ? target.name + " @ " + target.peer_name : target.name;
            document.getElementById("user_name_display").textContent = target.name;

            if (target.avatar) {
                var av = target.avatar;
                if (!av.startsWith("http") && !av.startsWith("/")) av = _bp + "/img/" + av;
                document.getElementById("user_avatar").src = av;
            }
            if (target.peer_name && peer_el) {
                peer_el.textContent = "@ " + target.peer_name;
                peer_el.classList.remove("hidden");
            }
            if (!target.peer_id) {
                request("read_user", { id: target.id }).then(function (d) {
                    if (d.ok && d.user && d.user.created)
                        document.getElementById("user_joined").textContent =
                            "Joined " + new Date(d.user.created).toLocaleDateString();
                });
            }
            load_user_messages(target.id, target.peer_id || null);

            if (is_owner && !target.peer_id && admin_actions) {
                admin_actions.classList.remove("hidden");
                document.getElementById("admin_delete_btn").addEventListener("click", function () {
                    if (!confirm("Delete " + target.name + "? This cannot be undone.")) return;
                    request("delete_user", { id: target.id }).then(function (d) {
                        if (d.ok) { set_active_nav("users"); show_page("/users.html"); }
                        else show_error(d.reason);
                    });
                });
            }

            // Block / unblock — available to any logged-in user
            var bkey = _block_key(target.id, target.peer_id || null);
            var block_section = document.getElementById("block_actions");
            if (block_section) {
                block_section.classList.remove("hidden");
                var block_btn = document.getElementById("profile_block_btn");
                function _update_block_btn() {
                    var blocked = _blocked_users.has(bkey);
                    block_btn.textContent = blocked ? "Unblock" : "Block";
                    block_btn.className   = blocked ? "secondary" : "";
                }
                _update_block_btn();
                block_btn.addEventListener("click", function () {
                    if (_blocked_users.has(bkey)) {
                        _blocked_users.delete(bkey);
                    } else {
                        _blocked_users.add(bkey);
                    }
                    _save_blocked_users();
                    _update_block_btn();
                });
            }
        }
    }

    // ── Profile cache ──────────────────────────────────────────────────
    function request_profiles() {
        if (!needed_profiles.size) return;
        var ids = Array.from(needed_profiles);
        needed_profiles.clear();

        request("read_profiles", { ids: ids }).then(function (d) {
            if (!d.ok) return;
            d.users.forEach(function (u) { known_profiles[u.id] = u; });
            ids.forEach(function (id) {
                var profile = known_profiles[id];
                if (!profile) return;
                document.querySelectorAll("[data-uid='" + id + "']").forEach(function (el) {
                    if (el.classList.contains("sender") || el.classList.contains("stream-name"))
                        el.textContent = profile.name;
                });
            });
        });
    }

    // ── Public API ─────────────────────────────────────────────────────
    return {
        init:         init,
        open_channel: open_channel,
        drop_image:   drop_image,
        drop_member:  drop_member,
        drop_ban:   drop_ban,
    };

})();
