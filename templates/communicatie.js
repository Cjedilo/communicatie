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
    var current_channel_allow_replies    = true;
    var current_channel_edit_mode        = "off";
    var current_channel_post_restricted  = false;
    var current_channel_restrict_replies = true;
    var current_channel_allow_images     = true;
    var current_channel_allow_reactions  = true;
    var current_channel_allow_polls      = true;
    var current_channel_allow_markdown   = true;
    var current_channel_can_manage       = false;
    var _chat_origin         = "stream"; // nav page to return to on back
    var pending_parent_id    = null;
    var pending_image        = null;
    var pending_scroll_id    = null;
    var _pending_message_draft = null;  // pre-fills #message_input after open_channel()
    var _pending_chat_notice   = null;  // explains why an assisted chat was opened

    var known_profiles  = {};
    var needed_profiles = new Set();
    var _stream_items    = [];
    var _stream_has_more = false;
    var _stream_loading  = false;
    var _stream_observer = null;
    var _stream_muted      = new Set(); // private remote channels opted OUT
    var _stream_subscribed = new Set(); // public  remote channels opted IN
    var _blocked_users     = new Set(); // "user_id|peer_id" — never show messages from these users
    var _favorite_channels = new Set(); // "channel_id|peer_id" — starred channels
    var _unread = {};                   // "channel_id|peer_id" → unread count (int)
    var _mention_count    = 0;          // total unread mentions across all channels
    var _channel_mentions = {};         // "channel_id|peer_id" → mention count for badge math
    var _on_stream_page = false;        // true while stream page is visible

    function _save_stream_muted()      { send("set_user_setting", { key: "stream_muted",      value: Array.from(_stream_muted) }); }
    function _save_stream_subscribed() { send("set_user_setting", { key: "stream_subscribed", value: Array.from(_stream_subscribed) }); }
    function _save_blocked_users()     { send("set_user_setting", { key: "blocked_users",     value: Array.from(_blocked_users) }); }
    function _save_favorites()         { send("set_user_setting", { key: "favorite_channels", value: Array.from(_favorite_channels) }); }

    function _fav_key(channel_id, peer_id) { return (channel_id || "") + "|" + (peer_id || ""); }

    function _chan_key(channel_id, peer_id) { return (channel_id || "") + "|" + (peer_id || ""); }

    function _update_unread_badge() {
        var total = 0;
        Object.keys(_unread).forEach(function (k) { total += _unread[k] || 0; });
        set_nav_badge("stream", total > 0 ? String(total) : "");
    }

    function _update_mention_badge() {
        var show = _mention_count > 0;
        ["mention_badge", "mention_badge_mobile"].forEach(function (id) {
            var el = document.getElementById(id);
            if (el) el.classList.toggle("hidden", !show);
        });
    }

    function _mark_read(channel_id, peer_id) {
        var key = _chan_key(channel_id, peer_id);
        if (_unread[key]) {
            delete _unread[key];
            _update_unread_badge();
        }
        if (_channel_mentions[key]) {
            _mention_count = Math.max(0, _mention_count - _channel_mentions[key]);
            delete _channel_mentions[key];
            _update_mention_badge();
        }
        send("mark_channel_read", { channel_id: channel_id });
    }

    function _increment_unread(channel_id, peer_id) {
        var key = _chan_key(channel_id, peer_id);
        _unread[key] = (_unread[key] || 0) + 1;
        _update_unread_badge();
    }

    var _fav_star_buttons = {}; // key -> [{el, on_label}] — kept in sync across list/header

    function _register_fav_button(key, el) {
        (_fav_star_buttons[key] = _fav_star_buttons[key] || []).push(el);
    }

    function _set_favorite(key, on) {
        if (on) _favorite_channels.add(key);
        else    _favorite_channels.delete(key);
        _save_favorites();
        // Drop refs to buttons no longer in the document (stale chat-header
        // buttons from previously visited chats) while we're here.
        var live = (_fav_star_buttons[key] || []).filter(function (el) { return document.contains(el); });
        live.forEach(function (el) {
            el.textContent = on ? "⭐" : "☆";
            el.title = on ? "Remove from favorites" : "Add to favorites";
        });
        _fav_star_buttons[key] = live;
        _render_favorites_section();
    }

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
        if (data.type === "message_edited"  && data.ok) on_live_message_edited(data.message);
        if (data.type === "message_deleted" && data.ok) on_live_message_deleted(data.id);
        if (data.type === "reaction_updated" && data.ok) on_live_reaction_updated(data);
        if (data.type === "poll_updated"    && data.ok) on_live_poll_updated(data);
        if (data.type === "new_message_notification" && data.ok) {
            var nkey = _chan_key(data.channel_id, data.peer_id || null);
            if (!_on_stream_page && nkey !== _chan_key(current_channel_id, current_channel_peer)) {
                _increment_unread(data.channel_id, data.peer_id || null);
            }
            if ((data.mentions || []).indexOf(user_id) !== -1) {
                if (!_on_stream_page && nkey !== _chan_key(current_channel_id, current_channel_peer)) {
                    _mention_count++;
                    _channel_mentions[nkey] = (_channel_mentions[nkey] || 0) + 1;
                    _update_mention_badge();
                }
            }
        }
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

    // Mounts the emoji picker anchored to `anchor_el` (which must be
    // position:relative); calls on_pick(emoji) and tears itself down on pick
    // or on the next click anywhere else.
    function _open_icon_picker(anchor_el, on_pick) {
        if (document.getElementById("icon_picker")) return;
        var wrap = document.createElement("div");
        wrap.id = "icon_picker";
        wrap.className = "icon-picker-wrap";
        anchor_el.appendChild(wrap);

        function _mount_picker(EmojiPickerEl) {
            var picker = new EmojiPickerEl();
            picker.dataSource = _bp + "/emoji-data.json";
            wrap.appendChild(picker);
            wrap.addEventListener("emoji-click", function (ev) {
                var em = ev.detail.unicode;
                if (!em) return;
                wrap.remove();
                on_pick(em);
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
    }

    // Shared "new channel" / "edit channel" overlay — works on whichever page
    // (channels.html or chat.html) currently has the #new_channel_overlay
    // markup in the DOM. Pass an existing channel to edit it (name stays
    // fixed — renaming was never an option), or omit it to create a new one.
    function open_channel_settings(existing, on_saved) {
        var overlay     = document.getElementById("new_channel_overlay");
        var title_el    = document.getElementById("new_channel_overlay_title");
        var close_btn   = document.getElementById("close_new_channel");
        var name_input  = document.getElementById("channel_name");
        var icon_el     = document.getElementById("new_channel_icon");
        var desc_input    = document.getElementById("channel_description");
        var private_cb    = document.getElementById("channel_private");
        var replies_cb    = document.getElementById("channel_allow_replies");
        var restricted_cb = document.getElementById("channel_post_restricted");
        var restrict_replies_cb  = document.getElementById("channel_restrict_replies");
        var restrict_replies_row = document.getElementById("channel_restrict_replies_row");
        var images_cb     = document.getElementById("channel_allow_images");
        var reactions_cb   = document.getElementById("channel_allow_reactions");
        var polls_cb       = document.getElementById("channel_allow_polls");
        var markdown_cb    = document.getElementById("channel_allow_markdown");
        var edit_mode_sel  = document.getElementById("channel_edit_mode");
        var submit_btn    = document.getElementById("channel_form_submit");
        var form          = document.getElementById("create_channel_form");
        if (!overlay) return;

        function _sync_restrict_replies_visibility() {
            var relevant = restricted_cb.checked && replies_cb.checked;
            restrict_replies_row.classList.toggle("hidden", !relevant);
        }
        restricted_cb.onchange = _sync_restrict_replies_visibility;
        replies_cb.onchange    = _sync_restrict_replies_visibility;

        var is_edit  = !!existing;
        var icon_val = is_edit ? _channel_icon(existing) : "🗨️";

        title_el.textContent   = is_edit ? "Edit channel — " + existing.name : "New channel";
        submit_btn.textContent = is_edit ? "Save" : "Create";
        name_input.classList.remove("hidden");
        name_input.required = !is_edit;
        name_input.readOnly = is_edit;
        name_input.value    = is_edit ? existing.name : "";
        desc_input.value     = is_edit ? (existing.description || "") : "";
        private_cb.checked    = is_edit ? !existing.public : false;
        replies_cb.checked    = is_edit ? (existing.allow_replies !== false && existing.allow_replies !== 0) : true;
        restricted_cb.checked = is_edit ? !!existing.post_restricted : false;
        restrict_replies_cb.checked = is_edit ? (existing.restrict_replies !== false && existing.restrict_replies !== 0) : true;
        images_cb.checked     = is_edit ? (existing.allow_images !== false && existing.allow_images !== 0) : true;
        reactions_cb.checked  = is_edit ? (existing.allow_reactions !== false && existing.allow_reactions !== 0) : true;
        polls_cb.checked      = is_edit ? (existing.allow_polls     !== false && existing.allow_polls     !== 0) : true;
        markdown_cb.checked   = is_edit ? (existing.allow_markdown  !== false && existing.allow_markdown  !== 0) : true;
        edit_mode_sel.value   = is_edit ? (existing.edit_mode || "off") : "off";
        icon_el.textContent = icon_val;
        _sync_restrict_replies_visibility();

        icon_el.onclick = function (e) {
            e.stopPropagation();
            _open_icon_picker(icon_el, function (em) {
                icon_val = em;
                icon_el.textContent = em;
            });
        };

        overlay.classList.remove("hidden");
        if (!is_edit) name_input.focus();

        close_btn.onclick = function () { overlay.classList.add("hidden"); };

        form.onsubmit = function (e) {
            e.preventDefault();
            var is_private       = private_cb.checked;
            var allow_replies    = replies_cb.checked;
            var post_restricted  = restricted_cb.checked;
            var restrict_replies = restrict_replies_cb.checked;
            var allow_images     = images_cb.checked;
            var allow_reactions  = reactions_cb.checked;
            var allow_polls      = polls_cb.checked;
            var allow_markdown   = markdown_cb.checked;
            var edit_mode        = edit_mode_sel.value;
            var description      = desc_input.value.trim();

            if (is_edit) {
                Promise.all([
                    request("set_channel_icon",          { channel_id: existing.id, peer_id: existing.peer_id || null, icon: icon_val }),
                    request("set_channel_public",        { channel_id: existing.id, peer_id: existing.peer_id || null, public: !is_private }),
                    request("set_channel_allow_replies",  { channel_id: existing.id, peer_id: existing.peer_id || null, allow_replies: allow_replies }),
                    request("set_channel_post_restricted", { channel_id: existing.id, peer_id: existing.peer_id || null, post_restricted: post_restricted }),
                    request("set_channel_restrict_replies", { channel_id: existing.id, peer_id: existing.peer_id || null, restrict_replies: restrict_replies }),
                    request("set_channel_allow_images",    { channel_id: existing.id, peer_id: existing.peer_id || null, allow_images: allow_images }),
                    request("set_channel_allow_reactions", { channel_id: existing.id, peer_id: existing.peer_id || null, allow_reactions: allow_reactions }),
                    request("set_channel_allow_polls",     { channel_id: existing.id, peer_id: existing.peer_id || null, allow_polls: allow_polls }),
                    request("set_channel_allow_markdown",  { channel_id: existing.id, peer_id: existing.peer_id || null, allow_markdown: allow_markdown }),
                    request("set_channel_edit_mode",       { channel_id: existing.id, peer_id: existing.peer_id || null, edit_mode: edit_mode }),
                    request("set_channel_description",     { channel_id: existing.id, peer_id: existing.peer_id || null, description: description }),
                ]).then(function (results) {
                    var failed = results.find(function (r) { return !r.ok; });
                    if (failed) { show_error(failed.reason); return; }
                    overlay.classList.add("hidden");
                    if (on_saved) on_saved({
                        id: existing.id, icon: icon_val, public: !is_private,
                        allow_replies: allow_replies, post_restricted: post_restricted,
                        restrict_replies: restrict_replies, allow_images: allow_images,
                        allow_reactions: allow_reactions, allow_polls: allow_polls,
                        allow_markdown: allow_markdown,
                        edit_mode: edit_mode, description: description,
                    });
                });
            } else {
                var name = name_input.value.trim();
                if (!name) return;
                request("create_channel", {
                    name: name, public: !is_private, icon: icon_val,
                    allow_replies: allow_replies, post_restricted: post_restricted,
                    restrict_replies: restrict_replies, allow_images: allow_images,
                    allow_reactions: allow_reactions, allow_polls: allow_polls,
                    allow_markdown: allow_markdown,
                    edit_mode: edit_mode, description: description,
                }).then(function (d) {
                    if (!d.ok) { show_error(d.reason); return; }
                    overlay.classList.add("hidden");
                    if (on_saved) on_saved(d.channel);
                });
            }
        };
    }

    function make(tag, cls, text) {
        var el = document.createElement(tag);
        if (cls)  el.className = cls;
        if (text) el.appendChild(document.createTextNode(text));
        return el;
    }

    function _initial_avatar(name) {
        var letter = ((name || "?")[0]).toUpperCase();
        var h = 0;
        var s = name || "?";
        for (var i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
        var hue = Math.abs(h) % 360;
        var bg  = "hsl(" + hue + ",55%,42%)";
        var svg = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 40 40">'
            + '<circle cx="20" cy="20" r="20" fill="' + bg + '"/>'
            + '<text x="20" y="27" text-anchor="middle" font-family="system-ui,sans-serif"'
            + ' font-size="19" font-weight="700" fill="white">' + letter + '</text>'
            + '</svg>';
        return "data:image/svg+xml," + encodeURIComponent(svg);
    }

    function avatar_img(src, cls, name) {
        var img = document.createElement("img");
        img.className = cls || "avatar";
        var resolved = src
            ? (src.startsWith("http") || src.startsWith("/")) ? src : _bp + "/img/" + src
            : "";
        var fallback = _initial_avatar(name || "?");
        img.src = resolved || fallback;
        img.onerror = function () { this.src = fallback; this.onerror = null; };
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
        _on_stream_page  = false;
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
                var favs = d.settings.favorite_channels;
                if (Array.isArray(favs)) favs.forEach(function (k) { _favorite_channels.add(k); });
            });
            // Populate _known_channels and initial unread counts at startup so
            // new_message_notification badges work on any page, not just after
            // visiting the channels list.
            request("read_channels").then(function (d) {
                if (!d.ok) return;
                var mentions = 0;
                d.channels.forEach(function (ch) {
                    var key = _chan_key(ch.id, null);
                    if (ch.unread_count  > 0) _unread[key] = ch.unread_count;
                    if (ch.mention_count > 0) { _channel_mentions[key] = ch.mention_count; mentions += ch.mention_count; }
                });
                _mention_count = mentions;
                _update_unread_badge();
                _update_mention_badge();
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

            // Arrived via a shared chat link (/join/<id> → ?join=<id>)
            var join_id = new URLSearchParams(window.location.search).get("join");
            if (join_id) {
                history.replaceState(null, "", _bp + "/");
                request("read_channel", { id: join_id, peer_id: null }).then(function (d) {
                    if (d.ok) {
                        open_channel(join_id, null);
                    } else if (d.owner) {
                        start_assisted_chat(d.owner.id, null, d.owner.name,
                            "Hi! Could I get access to \"" + (d.channel_name || "this chat") + "\"?",
                            "🔒 You don't have access to \"" + (d.channel_name || "this chat") +
                            "\" — here's a draft message to ask " + d.owner.name + " for access. Review it and hit send.");
                    } else {
                        show_error(d.reason);
                    }
                });
            }
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

        // Arrived via a shared chat link while logged out — show context
        var params    = new URLSearchParams(window.location.search);
        var join_id   = params.get("join");
        var join_name = params.get("join_name");
        var banner    = document.getElementById("join_banner");
        if (join_id && join_name && banner) {
            var is_pub = params.get("join_public") === "1";
            document.getElementById("join_banner_text").textContent =
                "You're joining " + (is_pub ? "🌐 " : "🔒 ") + join_name + " — log in or register to continue.";
            banner.classList.remove("hidden");

            document.getElementById("join_elsewhere_btn").addEventListener("click", function () {
                var url = window.location.origin + _bp + "/join/" + join_id;
                var msg = "Paste this on your own server, under Channels → 📤 Open link:";
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(url).then(function () {
                        show_error("Link copied! " + msg);
                    }).catch(function () { prompt(msg, url); });
                } else {
                    prompt(msg, url);
                }
            });
        }
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
        return _stream_subscribed.has(key);       // remote: always opt-in (public or private)
    }

    function on_stream_message(msg) {
        if (!_stream_visible(msg)) return;
        if (!_stream_seen().has(msg.id)) {
            _stream_items.push({ t: new Date(msg.created), data: msg });
            render_stream(true); // live message appears at top → anchor existing content
        }
        // Visible in stream → count as read so badge doesn't accumulate.
        _mark_read(msg.channel_id, msg.peer_id || null);
    }

    function on_stream_update(messages) {
        var seen = _stream_seen();
        var added = false;
        (messages || []).forEach(function (msg) {
            if (!_stream_visible(msg)) return;
            if (!seen.has(msg.id)) { seen.add(msg.id); _stream_items.push({ t: new Date(msg.created), data: msg }); added = true; }
            _mark_read(msg.channel_id, msg.peer_id || null);
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
        _on_stream_page  = true;
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
                    // Remote channel: all use _stream_subscribed (opt-in)
                    _stream_subscribed.delete(mute_key);
                    _save_stream_subscribed();
                    _stream_items = _stream_items.filter(function (i) { return _stream_visible(i.data); });
                    render_stream();
                }
            });
            badge.appendChild(tbtn);
            div.appendChild(badge);
        }

        var row = make("div", "stream-row");
        var _sp = known_profiles[msg.sender_user_id] || {};
        row.appendChild(avatar_img(
            _sp.avatar || msg.sender_avatar,
            null,
            msg.sender_name || _sp.name || _sp.display_name
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
            if (msg.image && !msg.text) { preview.appendChild(document.createTextNode("📷 Image")); }
            else { preview.appendChild(render_markdown(msg.text)); }
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
                    if (m.image && !m.text) { preview.appendChild(document.createTextNode("📷 Image")); }
                    else { preview.appendChild(render_markdown(m.text)); }
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
        _on_stream_page  = false;
        _close_mention_popup();
        _mention_users = null;  // refresh user list per channel
        var active = document.querySelector(".nav-item.active");
        _chat_origin = (active && active.dataset.page) || "stream";
        current_channel_id   = channel_id;
        current_channel_peer = peer_id || null;
        current_channel_allow_replies    = true;
        current_channel_edit_mode        = "off";
        current_channel_post_restricted  = false;
        current_channel_restrict_replies = true;
        current_channel_allow_images     = true;
        current_channel_allow_reactions  = true;
        current_channel_allow_polls      = true;
        current_channel_allow_markdown   = true;
        current_channel_can_manage       = false;
        pending_parent_id    = null;
        pending_image        = null;
        pending_scroll_id    = scroll_to_id || null;
        _mark_read(channel_id, peer_id || null);
        // Auto-subscribe remote channels to stream when opened — opening implies membership.
        if (peer_id) {
            var skey = _stream_muted_key({ channel_id: channel_id, peer_id: peer_id });
            if (!_stream_subscribed.has(skey)) {
                _stream_subscribed.add(skey);
                _save_stream_subscribed();
            }
        }

        send("unsubscribe_all");
        document.body.classList.add("in-chat");

        show_page("/chat.html", function () {
            setup_chat_page();
            load_channel();
        });
    }

    // Start (or open) a DM with someone and pre-fill a draft message they
    // must still review and send themselves — used for "ask owner for
    // access" and "ask my admin to connect" flows.
    function start_assisted_chat(target_user_id, target_peer_id, target_name, draft_text, notice_text) {
        request("start_chat", {
            user_id: target_user_id, peer_id: target_peer_id || null, name: target_name || null,
        }).then(function (r) {
            if (!r.ok) { show_error(r.reason); return; }
            _pending_message_draft = draft_text || null;
            _pending_chat_notice   = notice_text || null;
            open_channel(r.channel_id, null);
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

    // ── @mention autocomplete ──────────────────────────────────────────
    var _mention_users     = null;  // cached user list; null = not yet fetched
    var _mention_popup_el  = null;  // active popup DOM element
    var _mention_sel       = -1;    // keyboard-selected index
    var _mention_query     = "";    // current partial name after @
    var _mention_at_pos    = -1;    // caret position of the @ character

    function _ensure_mention_users() {
        if (_mention_users !== null) return;
        _mention_users = [];
        // Seed from profiles already seen in this session.
        Object.keys(known_profiles).forEach(function (uid) {
            var u = known_profiles[uid];
            if (uid !== user_id) _mention_users.push(u);
        });
        // Fetch all local users for completeness (deduplicated by id).
        request("read_users").then(function (d) {
            if (!d.ok) return;
            var seen = new Set(_mention_users.map(function (u) { return String(u.id); }));
            (d.users || []).forEach(function (u) {
                if (!seen.has(String(u.id)) && String(u.id) !== user_id) {
                    _mention_users.push(u);
                    seen.add(String(u.id));
                }
            });
        });
    }

    function _mention_name(u) { return u.display_name || u.name || ""; }

    function _close_mention_popup() {
        if (_mention_popup_el) { _mention_popup_el.remove(); _mention_popup_el = null; }
        _mention_sel   = -1;
        _mention_query = "";
        _mention_at_pos = -1;
    }

    function _open_mention_popup(textarea, query) {
        _mention_query = query;
        var q = query.toLowerCase();
        var matches = (_mention_users || []).filter(function (u) {
            var n = _mention_name(u).toLowerCase();
            return n.startsWith(q);
        }).slice(0, 6);

        if (!matches.length) { _close_mention_popup(); return; }

        if (!_mention_popup_el) {
            _mention_popup_el = make("div", "mention-popup");
            textarea.parentNode.insertBefore(_mention_popup_el, textarea);
        }
        _mention_popup_el.replaceChildren();
        if (_mention_sel >= matches.length) _mention_sel = 0;

        matches.forEach(function (u, idx) {
            var li = make("div", "mention-item" + (idx === _mention_sel ? " sel" : ""), "@" + _mention_name(u));
            li.addEventListener("mousedown", function (e) {
                e.preventDefault();
                _insert_mention(textarea, _mention_name(u));
            });
            _mention_popup_el.appendChild(li);
        });
    }

    function _insert_mention(textarea, name) {
        var val = textarea.value;
        var before = val.slice(0, _mention_at_pos) + "@" + name + " ";
        var after  = val.slice(textarea.selectionStart);
        textarea.value = before + after;
        var pos = before.length;
        textarea.setSelectionRange(pos, pos);
        _close_mention_popup();
        textarea.focus();
    }

    function _on_mention_input(textarea) {
        if (!current_channel_allow_markdown) { _close_mention_popup(); return; }
        var caret = textarea.selectionStart;
        var before = textarea.value.slice(0, caret);
        var m = before.match(/@([\w]*)$/);
        if (!m) { _close_mention_popup(); return; }
        _mention_at_pos = caret - m[0].length;
        _ensure_mention_users();
        _open_mention_popup(textarea, m[1]);
    }

    function _on_mention_keydown(e, textarea) {
        if (!_mention_popup_el) return false;
        var items = _mention_popup_el.querySelectorAll(".mention-item");
        if (e.key === "ArrowDown") {
            e.preventDefault();
            _mention_sel = (_mention_sel + 1) % items.length;
            items.forEach(function (el, i) { el.classList.toggle("sel", i === _mention_sel); });
            return true;
        }
        if (e.key === "ArrowUp") {
            e.preventDefault();
            _mention_sel = (_mention_sel - 1 + items.length) % items.length;
            items.forEach(function (el, i) { el.classList.toggle("sel", i === _mention_sel); });
            return true;
        }
        if (e.key === "Enter" || e.key === "Tab") {
            if (_mention_sel >= 0 && _mention_sel < items.length) {
                e.preventDefault();
                var name = items[_mention_sel].textContent.slice(1); // strip leading @
                _insert_mention(textarea, name);
                return true;
            }
        }
        if (e.key === "Escape") {
            _close_mention_popup();
            return true;
        }
        return false;
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
        document.getElementById("poll_btn").addEventListener("click", open_create_poll);
        var inp = document.getElementById("message_input");
        inp.addEventListener("input",   function ()  { _on_mention_input(inp); });
        inp.addEventListener("keydown", function (e) {
            if (_on_mention_keydown(e, inp)) return;
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                send_message();
            }
        });
        inp.addEventListener("blur", function () {
            // Delay so mousedown on popup items fires first.
            setTimeout(_close_mention_popup, 150);
        });
    }

function load_channel() {
        request("read_channel", {
            id:      current_channel_id,
            peer_id: current_channel_peer,
        }).then(function (d) {
            if (!d.ok) {
                if (d.owner) {
                    exit_chat();
                    set_active_nav("stream");
                    start_assisted_chat(d.owner.id, d.peer_id || null, d.owner.name,
                        "Hi! Could I get access to \"" + (d.channel_name || "this chat") + "\"?",
                        "🔒 You don't have access to \"" + (d.channel_name || "this chat") +
                        "\" — here's a draft message to ask " + d.owner.name + " for access. Review it and hit send.");
                    return;
                }
                show_error(d.reason); return;
            }

            var chan_name = (d.channel && d.channel.name) || "";
            var title = document.getElementById("chat_title");
            if (title) {
                title.textContent = chan_name;
                if (d.channel && typeof d.channel.public !== "undefined") {
                    var vis = make("span", "chat-visibility", d.channel.public ? " 🌐" : " 🔒");
                    vis.title = d.channel.public ? "Public channel" : "Private channel";
                    title.appendChild(vis);
                }
                // Only present for real channels — DMs have no meaningful owner
                if (d.channel && d.channel.created_by_name) {
                    title.appendChild(make("span", "chat-creator-tag",
                        " · Created by " + d.channel.created_by_name));
                }
                if (d.channel && d.channel.poster_count) {
                    title.appendChild(make("span", "chat-creator-tag",
                        " · " + d.channel.poster_count + " posters"));
                }
            }
            if (chan_name) document.title = chan_name;

            var ch = d.channel || {};
            ch.peer_id = current_channel_peer || null;
            var desc_el = document.getElementById("chat_description");
            if (desc_el) {
                desc_el.replaceChildren(); if (ch.description) desc_el.appendChild(render_markdown(ch.description));
                desc_el.classList.toggle("hidden", !ch.description);
            }
            current_channel_allow_replies    = ch.allow_replies !== false && ch.allow_replies !== 0;
            current_channel_edit_mode        = ch.edit_mode || "off";
            current_channel_post_restricted  = !!ch.post_restricted;
            current_channel_restrict_replies = ch.restrict_replies !== false && ch.restrict_replies !== 0;
            current_channel_allow_images     = ch.allow_images !== false && ch.allow_images !== 0;
            current_channel_allow_reactions  = ch.allow_reactions !== false && ch.allow_reactions !== 0;
            current_channel_allow_polls      = ch.allow_polls     !== false && ch.allow_polls     !== 0;
            current_channel_allow_markdown   = ch.allow_markdown  !== false && ch.allow_markdown  !== 0;
            current_channel_can_manage       = !!ch.can_manage;
            _update_compose_visibility();
            _apply_image_restriction();

            var share_btn = document.getElementById("share_btn");
            if (share_btn) {
                if (!d.host_address) {
                    share_btn.classList.add("hidden");
                } else {
                    share_btn.classList.remove("hidden");
                    share_btn.onclick = function () { _copy_share_link(d.host_address, current_channel_id); };
                }
            }

            var icon_el = document.getElementById("chat_icon");
            if (icon_el) icon_el.textContent = _channel_icon(ch);

            var container = document.getElementById("messages");
            container.replaceChildren();
            if (_pending_chat_notice) {
                container.appendChild(make("div", "chat-notice", _pending_chat_notice));
                _pending_chat_notice = null;
            }
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

            setup_chat_buttons(d.channel);

            if (_pending_message_draft) {
                var input = document.getElementById("message_input");
                if (input) input.value = _pending_message_draft;
                _pending_message_draft = null;
            }
        });
    }

    // Hides the message input for everyone except the channel's managers
    // when the channel is in "only moderators can post" mode. If replies
    // aren't also restricted, non-managers still get the input while
    // composing a reply — just not for starting new top-level messages.
    function _update_compose_visibility() {
        var input_area = document.getElementById("input_area");
        var notice      = document.getElementById("post_restricted_notice");
        if (!input_area || !notice) return;

        if (!current_channel_post_restricted || current_channel_can_manage) {
            input_area.classList.remove("hidden");
            notice.classList.add("hidden");
            return;
        }
        if (current_channel_restrict_replies || !current_channel_allow_replies) {
            input_area.classList.add("hidden");
            notice.textContent = "🔒 Only moderators can post in this channel";
            notice.classList.remove("hidden");
            return;
        }
        var composing_reply = !!pending_parent_id;
        input_area.classList.toggle("hidden", !composing_reply);
        notice.textContent = "🔒 Only moderators can start new topics — reply to an existing message instead";
        notice.classList.toggle("hidden", composing_reply);
    }

    function _apply_image_restriction() {
        var dz = document.getElementById("drop_zone");
        if (dz) dz.classList.toggle("hidden", !current_channel_allow_images);
        var pb = document.getElementById("poll_btn");
        if (pb) pb.classList.toggle("hidden", !current_channel_allow_polls);
    }

    function setup_chat_buttons(channel) {
        var is_pub = channel && channel.public;

        // Favorite toggle
        var fbtn = document.getElementById("fav_btn");
        if (fbtn && channel) {
            var cfkey = _fav_key(channel.id, current_channel_peer);
            fbtn.classList.remove("hidden");
            fbtn.textContent = _favorite_channels.has(cfkey) ? "⭐" : "☆";
            fbtn.title = _favorite_channels.has(cfkey) ? "Remove from favorites" : "Add to favorites";
            fbtn.addEventListener("click", function () { _set_favorite(cfkey, !_favorite_channels.has(cfkey)); });
            _register_fav_button(cfkey, fbtn);
        }

        // Edit channel (icon / private / replies) — managers only
        var ebtn = document.getElementById("edit_channel_btn");
        if (ebtn && channel && channel.can_manage) {
            ebtn.classList.remove("hidden");
            ebtn.addEventListener("click", function () {
                open_channel_settings(channel, function (updated) {
                    channel.icon             = updated.icon;
                    channel.public           = updated.public;
                    channel.allow_replies    = updated.allow_replies;
                    channel.post_restricted  = updated.post_restricted;
                    channel.restrict_replies = updated.restrict_replies;
                    channel.allow_images     = updated.allow_images;
                    channel.allow_reactions  = updated.allow_reactions;
                    channel.edit_mode        = updated.edit_mode;
                    channel.description      = updated.description;
                    current_channel_allow_replies    = updated.allow_replies;
                    current_channel_edit_mode        = updated.edit_mode;
                    current_channel_post_restricted  = updated.post_restricted;
                    current_channel_restrict_replies = updated.restrict_replies;
                    current_channel_allow_images     = updated.allow_images;
                    channel.allow_reactions  = updated.allow_reactions;
                    channel.allow_polls      = updated.allow_polls;
                    channel.allow_markdown   = updated.allow_markdown;
                    current_channel_allow_reactions  = updated.allow_reactions;
                    current_channel_allow_polls      = updated.allow_polls;
                    current_channel_allow_markdown   = updated.allow_markdown;
                    _update_compose_visibility();
                    _apply_image_restriction();
                    var icon_el = document.getElementById("chat_icon");
                    if (icon_el) icon_el.textContent = _channel_icon(channel);
                    var vis_el = document.querySelector("#chat_title .chat-visibility");
                    if (vis_el) {
                        vis_el.textContent = channel.public ? " 🌐" : " 🔒";
                        vis_el.title = channel.public ? "Public channel" : "Private channel";
                    }
                    var desc_el = document.getElementById("chat_description");
                    if (desc_el) {
                        desc_el.replaceChildren(); if (channel.description) desc_el.appendChild(render_markdown(channel.description));
                        desc_el.classList.toggle("hidden", !channel.description);
                    }
                });
            });
        }

        // Members button — private channels: full member management;
        // public channels: moderator management only (for managers).
        var btn = document.getElementById("members_btn");
        var show_members_btn = !is_pub || (channel && channel.can_manage);
        if (btn && show_members_btn) {
            btn.classList.remove("hidden");
            btn.textContent = is_pub ? "Moderators" : "Members";
            btn.addEventListener("click", function () {
                request("read_members", { channel_id: current_channel_id }).then(function (d) {
                    if (!d.ok) { show_error(d.reason); return; }
                    if (d.can_manage && d.is_public) {
                        open_moderator_manager(d.members, channel.name, d.is_creator);
                    } else if (d.can_manage) {
                        // Reset drop handlers to member mode
                        document.getElementById("members_list").setAttribute(
                            "ondrop", "communicatie.drop_member(event, true)");
                        document.getElementById("non_members_list").setAttribute(
                            "ondrop", "communicatie.drop_member(event, false)");
                        open_member_manager(d.members, d.non_members, channel.name, d.is_creator);
                    } else {
                        open_readonly_list("Members", d.members, channel.name);
                    }
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
                    if (d.can_manage) {
                        open_ban_manager(d.banned, d.not_banned, channel.name);
                    } else {
                        open_readonly_list("Banned users", d.banned, channel.name);
                    }
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
        div._msg = msg;   // live reference so push updates can mutate it in place

        var _profile_av = known_profiles[msg.sender_user_id] || {};
        var img = avatar_img(
            _profile_av.avatar || msg.sender_avatar,
            null,
            msg.sender_name || _profile_av.name || _profile_av.display_name
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

        var edited_el = make("span", "edited-badge hidden", "(edited)");
        if (current_channel_edit_mode === "history") {
            edited_el.classList.add("clickable");
            edited_el.title = "Click to view earlier versions";
            edited_el.addEventListener("click", function () { toggle_message_history(msg, content_wrap); });
        }
        header.appendChild(edited_el);
        body.appendChild(header);

        var content_wrap = make("div", "content");
        body.appendChild(content_wrap);

        var needs_fetch = msg.text === null && msg.image === null;
        if (needs_fetch) {
            content_wrap.appendChild(make("span", "unavailable", "Loading…"));
            fetch_remote_content(msg, content_wrap);
        } else {
            render_content(content_wrap, msg);
        }
        if (msg.edited_at) edited_el.classList.remove("hidden");

        var reactions_el = make("div", "reactions hidden");
        body.appendChild(reactions_el);
        render_reactions(msg, reactions_el);

        var actions = make("div", "actions");
        var can_reply = current_channel_allow_replies && !(current_channel_post_restricted
            && current_channel_restrict_replies && !current_channel_can_manage);
        if (can_reply) {
            var reply = make("button", null, "↩ Reply");
            reply.addEventListener("click", function () { start_reply(msg); });
            actions.appendChild(reply);
        }
        if (current_channel_allow_reactions) {
            var react_btn = make("button", "react-btn", "🙂+");
            react_btn.title = "Add reaction";
            react_btn.addEventListener("click", function (e) {
                e.stopPropagation();
                _open_icon_picker(react_btn, function (emoji) { toggle_reaction(msg, emoji); });
            });
            actions.appendChild(react_btn);
        }
        if (msg.sender_user_id === user_id && current_channel_edit_mode !== "off") {
            var edit_btn = make("button", null, "✎ Edit");
            edit_btn.addEventListener("click", function () { start_edit(msg, content_wrap, edited_el); });
            actions.appendChild(edit_btn);
        }
        var can_delete = (msg.sender_user_id === user_id && !msg.sender_peer_id)
            || current_channel_can_manage;
        if (can_delete) {
            var del_btn = make("button", "msg-delete", "✕");
            del_btn.title = "Delete message";
            del_btn.addEventListener("click", function () {
                if (!confirm("Delete this message?")) return;
                request("delete_message", {
                    id:         msg.id,
                    channel_id: current_channel_id,
                    peer_id:    current_channel_peer || null,
                }).then(function (d) {
                    if (!d.ok) show_error(d.reason || "Could not delete message");
                });
            });
            actions.appendChild(del_btn);
        }
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

    function render_reactions(msg, el) {
        el.replaceChildren();
        var reactions = msg.reactions || [];
        el.classList.toggle("hidden", reactions.length === 0);
        reactions.forEach(function (r) {
            var mine = r.users.indexOf(user_id) !== -1;
            var pill = make("span", "reaction-pill" + (mine ? " mine" : ""), r.emoji + " " + r.users.length);
            pill.title = mine ? "Click to remove your reaction" : "Click to react";
            pill.addEventListener("click", function () { toggle_reaction(msg, r.emoji); });
            el.appendChild(pill);
        });
    }

    function toggle_reaction(msg, emoji) {
        request("toggle_reaction", {
            id:         msg.id,
            channel_id: current_channel_id,
            peer_id:    current_channel_peer || null,
            emoji:      emoji,
        }).then(function (d) {
            if (!d.ok) { show_error(d.reason || "Could not react"); return; }
            msg.reactions = d.reactions;
            var el = document.querySelector("[data-id='" + msg.id + "'] .reactions");
            if (el) render_reactions(msg, el);
        });
    }

    function on_live_reaction_updated(data) {
        var el = document.querySelector("[data-id='" + data.id + "']");
        if (!el || !el._msg) return;
        el._msg.reactions = data.reactions;
        var rel = el.querySelector(".reactions");
        if (rel) render_reactions(el._msg, rel);
    }

    function render_poll(msg, body) {
        var data;
        try { data = JSON.parse(msg.text); } catch (e) { body.textContent = msg.text; return; }
        var question = data.question || "";
        var options  = Array.isArray(data.options) ? data.options : [];
        var pv       = msg.poll_votes || {};
        var counts   = pv.counts || {};
        var my_vote  = (pv.my_vote !== undefined && pv.my_vote !== null) ? pv.my_vote : -1;
        var total    = Object.values(counts).reduce(function (a, b) { return a + b; }, 0);

        var card = make("div", "poll-card");
        card.appendChild(make("div", "poll-question", question));
        var opts_el = make("div", "poll-options");
        options.forEach(function (opt, i) {
            var cnt = counts[i] || 0;
            var pct = total > 0 ? Math.round(cnt / total * 100) : 0;
            var btn = make("button", "poll-option" + (i === my_vote ? " voted" : ""));
            var bar = make("div", "poll-bar");
            bar.style.width = pct + "%";
            btn.appendChild(bar);
            btn.appendChild(make("span", "poll-option-label", opt));
            btn.appendChild(make("span", "poll-option-count", cnt + " (" + pct + "%)"));
            btn.addEventListener("click", function () { poll_vote_click(msg, i); });
            opts_el.appendChild(btn);
        });
        card.appendChild(opts_el);
        card.appendChild(make("div", "poll-total", total + " vote" + (total !== 1 ? "s" : "")));
        body.appendChild(card);
    }

    function poll_vote_click(msg, option_index) {
        request("poll_vote", {
            id:           msg.id,
            channel_id:   current_channel_id,
            peer_id:      current_channel_peer || null,
            option_index: option_index,
        }).then(function (d) {
            if (!d.ok) { show_error(d.reason || "Could not vote"); return; }
            msg.poll_votes = { counts: d.counts || {}, my_vote: option_index };
            var el = document.querySelector("[data-id='" + msg.id + "'] .content");
            if (el) { el.replaceChildren(); render_poll(msg, el); }
        });
    }

    function on_live_poll_updated(data) {
        var el = document.querySelector("[data-id='" + data.id + "']");
        if (!el || !el._msg) return;
        var my_vote = el._msg.poll_votes ? el._msg.poll_votes.my_vote : -1;
        el._msg.poll_votes = { counts: data.counts || {}, my_vote: (my_vote !== undefined ? my_vote : -1) };
        var cel = el.querySelector(".content");
        if (cel) { cel.replaceChildren(); render_poll(el._msg, cel); }
    }

    function open_create_poll() {
        if (document.getElementById("poll_modal")) return;

        var modal = make("div", "poll-modal");
        modal.id = "poll_modal";
        var box = make("div", "poll-modal-box");
        box.appendChild(make("h3", null, "Create a poll"));

        var q_input = document.createElement("input");
        q_input.type = "text";
        q_input.placeholder = "Question…";
        q_input.maxLength = 256;
        box.appendChild(q_input);

        var opts_list = make("div", "poll-opts-list");
        function add_opt_row(val) {
            var row = make("div", "poll-opt-row");
            var inp = document.createElement("input");
            inp.type = "text";
            inp.placeholder = "Option…";
            inp.maxLength = 128;
            if (val) inp.value = val;
            var rm = make("button", "secondary", "✕");
            rm.title = "Remove";
            rm.addEventListener("click", function () {
                if (opts_list.children.length > 2) row.remove();
            });
            row.appendChild(inp);
            row.appendChild(rm);
            opts_list.appendChild(row);
        }
        add_opt_row(""); add_opt_row("");
        box.appendChild(opts_list);

        var add_btn = make("button", "secondary", "+ Add option");
        add_btn.addEventListener("click", function () {
            if (opts_list.children.length < 10) add_opt_row("");
        });
        box.appendChild(add_btn);

        var actions = make("div", "poll-modal-actions");
        var cancel = make("button", "secondary", "Cancel");
        cancel.addEventListener("click", function () { modal.remove(); });
        var submit = make("button", null, "Create poll");
        submit.addEventListener("click", function () {
            var question = q_input.value.trim();
            var options = Array.from(opts_list.querySelectorAll("input"))
                .map(function (i) { return i.value.trim(); })
                .filter(Boolean);
            if (!question) { show_error("Question is required"); return; }
            if (options.length < 2) { show_error("At least 2 options required"); return; }
            request("create_poll", {
                channel_id: current_channel_id,
                peer_id:    current_channel_peer || null,
                question:   question,
                options:    options,
            }).then(function (d) {
                if (!d.ok) { show_error(d.reason || "Could not create poll"); return; }
                modal.remove();
            });
        });
        actions.appendChild(cancel);
        actions.appendChild(submit);
        box.appendChild(actions);

        modal.appendChild(box);
        modal.addEventListener("click", function (e) { if (e.target === modal) modal.remove(); });
        document.body.appendChild(modal);
        q_input.focus();
    }

    function _esc(s) {
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    function _md_inline(s) {
        var saved = [];
        // protect inline code from further processing
        s = s.replace(/`([^`]+)`/g, function (_, c) { saved.push('<code>' + c + '</code>'); return '\x00' + (saved.length - 1) + '\x00'; });
        s = s.replace(/\*\*\*(.+?)\*\*\*/g, '<strong><em>$1</em></strong>');
        s = s.replace(/\*\*(.+?)\*\*/g,     '<strong>$1</strong>');
        s = s.replace(/\*(.+?)\*/g,         '<em>$1</em>');
        s = s.replace(/~~(.+?)~~/g,         '<del>$1</del>');
        s = s.replace(/\[([^\]]+)\]\((https?:\/\/[^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener noreferrer">$1</a>');
        s = s.replace(/(https?:\/\/[^\s<>"]+)/g, '<a href="$1" target="_blank" rel="noopener noreferrer">$1</a>');
        // Highlight @mentions of the current user (by login name and display name).
        var own = (known_profiles[user_id] || {});
        var own_names = [user_name, own.display_name, own.name].filter(Boolean);
        own_names.forEach(function (n) {
            var safe = n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            s = s.replace(new RegExp('@(' + safe + ')\\b', 'gi'),
                '<span class="mention-self">@$1</span>');
        });
        // Style all @mentions generically.
        s = s.replace(/@([\w]+)/g, function (m, name) {
            return '<span class="mention">@' + name + '</span>';
        });
        s = s.replace(/\x00(\d+)\x00/g, function (_, i) { return saved[+i]; });
        return s;
    }

    function render_markdown(text) {
        var el = document.createElement("div");
        el.className = "md";
        var s = _esc(text);

        // fenced code blocks  ```lang\n...\n```
        var blocks = [];
        s = s.replace(/```[^\n]*\n?([\s\S]*?)```/g, function (_, code) {
            blocks.push('<pre><code>' + code.replace(/\n$/, '') + '</code></pre>');
            return '\x01' + (blocks.length - 1) + '\x01';
        });

        var lines = s.split('\n'), out = [], i = 0;
        while (i < lines.length) {
            var ln = lines[i];

            // blockquote
            if (ln.startsWith('&gt; ') || ln === '&gt;') {
                var ql = [];
                while (i < lines.length && (lines[i].startsWith('&gt; ') || lines[i] === '&gt;')) {
                    ql.push(lines[i].replace(/^&gt; ?/, '')); i++;
                }
                out.push('<blockquote>' + ql.map(_md_inline).join('<br>') + '</blockquote>');
                continue;
            }
            // headers
            var hm = ln.match(/^(#{1,4}) (.*)/);
            if (hm) { var lv = hm[1].length; out.push('<h' + lv + '>' + _md_inline(hm[2]) + '</h' + lv + '>'); i++; continue; }
            // unordered list
            if (/^[*\-] /.test(ln)) {
                var ul = [];
                while (i < lines.length && /^[*\-] /.test(lines[i])) { ul.push('<li>' + _md_inline(lines[i].slice(2)) + '</li>'); i++; }
                out.push('<ul>' + ul.join('') + '</ul>');
                continue;
            }
            // ordered list
            if (/^\d+\. /.test(ln)) {
                var ol = [];
                while (i < lines.length && /^\d+\. /.test(lines[i])) { ol.push('<li>' + _md_inline(lines[i].replace(/^\d+\. /, '')) + '</li>'); i++; }
                out.push('<ol>' + ol.join('') + '</ol>');
                continue;
            }
            // blank line
            if (ln.trim() === '') { out.push('<br>'); i++; continue; }
            out.push('<p>' + _md_inline(ln) + '</p>');
            i++;
        }

        var html = out.join('');
        html = html.replace(/\x01(\d+)\x01/g, function (_, i) { return blocks[+i]; });
        el.innerHTML = html;
        return el;
    }

    function render_content(body, msg) {
        if ((msg.msg_type || "text") === "poll") {
            render_poll(msg, body);
            return;
        }
        if (msg.text) {
            body.appendChild(current_channel_allow_markdown
                ? render_markdown(msg.text)
                : (function () { var p = make("p", "text"); p.innerHTML = _md_inline(_esc(msg.text)); return p; }()));
        }
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
                msg.text  = d.message.text;
                msg.image = d.message.image;
                render_content(body, d.message);
            } else {
                body.appendChild(make("span", "unavailable", "Message unavailable"));
            }
        });
    }

    // Replaces a message's content with an inline textarea; Save sends the
    // edit (routed through the channel's host if it's remote), Cancel just
    // restores the previous content view.
    function start_edit(msg, content_wrap, edited_el) {
        if (content_wrap.parentNode.querySelector(".edit-form")) return;

        var prev_children = Array.prototype.slice.call(content_wrap.children);
        prev_children.forEach(function (c) { c.classList.add("hidden"); });

        var form = make("div", "edit-form");
        var textarea = document.createElement("textarea");
        textarea.value = msg.text || "";
        textarea.rows = 2;
        form.appendChild(textarea);

        var form_actions = make("div", "edit-form-actions");
        var save   = make("button", null, "Save");
        var cancel = make("button", "secondary", "Cancel");

        function close_form() {
            form.remove();
            prev_children.forEach(function (c) { c.classList.remove("hidden"); });
        }

        save.addEventListener("click", function () {
            var new_text = textarea.value.trim();
            if (!new_text && !msg.image) return;
            request("edit_message", {
                id: msg.id, peer_id: current_channel_peer,
                text: new_text || null, image: msg.image || null,
            }).then(function (d) {
                if (!d.ok) { show_error(d.reason); return; }
                msg.text      = d.message.text;
                msg.image     = d.message.image;
                msg.edited_at = d.message.edited_at;
                close_form();
                content_wrap.replaceChildren();
                render_content(content_wrap, msg);
                if (edited_el) edited_el.classList.remove("hidden");
            });
        });
        cancel.addEventListener("click", close_form);

        form_actions.appendChild(save);
        form_actions.appendChild(cancel);
        form.appendChild(form_actions);
        content_wrap.appendChild(form);
        textarea.focus();
    }

    // Shows/hides the list of prior versions for an edited message. Routes
    // to whichever server actually owns the content, same as content fetch.
    function toggle_message_history(msg, content_wrap) {
        var existing = content_wrap.parentNode.querySelector(".history-panel");
        if (existing) { existing.remove(); return; }

        var panel = make("div", "history-panel", "Loading…");
        content_wrap.insertAdjacentElement("afterend", panel);

        var is_remote = !!msg.sender_peer_id;
        var req = is_remote
            ? request("fetch_remote_message_history", {
                  message_id: msg.id, peer_id: msg.sender_peer_id, peer_address: msg.peer_address || "",
              })
            : request("read_message_history", { id: msg.id });

        req.then(function (d) {
            panel.replaceChildren();
            if (!d.ok || !d.history || !d.history.length) {
                panel.appendChild(make("span", "unavailable", "No earlier versions available"));
                return;
            }
            d.history.forEach(function (v) {
                var entry = make("div", "history-entry");
                entry.appendChild(make("span", "time", fmt_time(v.edited_at)));
                entry.appendChild(make("p", "text", v.text || (v.image ? "📷 Image" : "(no text)")));
                panel.appendChild(entry);
            });
        });
    }

    function start_reply(msg) {
        pending_parent_id = msg.id;
        _update_compose_visibility();
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
            _update_compose_visibility();
        });
        ctx.appendChild(label);
        ctx.appendChild(cancel);
        document.getElementById("message_input").focus();
    }

    function send_message() {
        var text = document.getElementById("message_input").value.trim();
        if (!text && !pending_image) return;
        var send_peer = current_channel_peer;

        request("message", {
            channel_id: current_channel_id,
            peer_id:    send_peer,
            text:       text || null,
            image:      pending_image || null,
            parent_id:  pending_parent_id || null,
        }).then(function (d) {
            if (d.ok) return;
            if (d.owner) {
                // Access was lost between opening the chat and sending — same
                // "ask for access" flow as opening a channel you can't read.
                start_assisted_chat(d.owner.id, send_peer, d.owner.name,
                    "Hi! Could I get access to \"" + (d.channel_name || "this chat") + "\"?",
                    "🔒 You no longer have access to \"" + (d.channel_name || "this chat") +
                    "\" — here's a draft message to ask " + d.owner.name + " for access. Review it and hit send.");
                return;
            }
            // Message never arrived — give it back rather than losing it,
            // unless the user has already started typing something new.
            var input = document.getElementById("message_input");
            if (input && !input.value) input.value = text;
            show_error(d.reason || "Could not send message");
        });

        document.getElementById("message_input").value = "";
        document.getElementById("reply_context").classList.add("hidden");
        document.getElementById("image_preview").classList.add("hidden");
        pending_parent_id = null;
        pending_image     = null;
        _update_compose_visibility();
    }

    function on_live_message(msg) {
        var container = document.getElementById("messages");
        if (msg.channel_id !== current_channel_id) return;
        if (!container) return;
        if (document.querySelector("[data-id='" + msg.id + "']")) return;
        append_message(container, msg);
        container.scrollTop = container.scrollHeight;
        request_profiles();
    }

    function on_live_message_edited(msg) {
        var container = document.getElementById("messages");
        if (!container) return;
        var el = container.querySelector("[data-id='" + msg.id + "']");
        if (!el || !el._msg) return;
        el._msg.text      = msg.text;
        el._msg.image     = msg.image;
        el._msg.edited_at = msg.edited_at;
        var content_wrap = el.querySelector(".content");
        if (content_wrap) {
            content_wrap.replaceChildren();
            render_content(content_wrap, el._msg);
        }
        var badge = el.querySelector(".edited-badge");
        if (badge) badge.classList.remove("hidden");
    }

    function on_live_message_deleted(id) {
        var container = document.getElementById("messages");
        if (!container) return;
        var el = container.querySelector("[data-id='" + id + "']");
        if (el) el.remove();
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

    function open_member_manager(members, non_members, chan_name, is_creator) {
        document.getElementById("manager_title").textContent  =
            chan_name ? "Members — " + chan_name : "Members";
        document.getElementById("col_left_title").textContent = "Members";
        document.getElementById("col_right_title").textContent = "Not members";
        var right_col = document.getElementById("member_manager_right_col");
        if (right_col) right_col.classList.remove("hidden");
        var mgr = document.getElementById("member_manager");
        mgr.classList.remove("hidden");

        var mlist  = document.getElementById("members_list");
        var nmlist = document.getElementById("non_members_list");
        mlist.replaceChildren();
        nmlist.replaceChildren();

        // Only the channel's actual creator can appoint/revoke moderators.
        members.forEach(function (u)     { mlist.appendChild(member_item(u, true, is_creator)); });
        non_members.forEach(function (u) { nmlist.appendChild(member_item(u)); });
        _setup_manager_filters(mlist, nmlist);
    }

    // Single-column moderator picker for public channels.
    function open_moderator_manager(users, chan_name, is_creator) {
        document.getElementById("manager_title").textContent =
            chan_name ? "Moderators — " + chan_name : "Moderators";
        document.getElementById("col_left_title").textContent = "Users";
        var right_col = document.getElementById("member_manager_right_col");
        if (right_col) right_col.classList.add("hidden");
        var mgr = document.getElementById("member_manager");
        mgr.classList.remove("hidden");

        var mlist = document.getElementById("members_list");
        mlist.removeAttribute("ondrop");
        mlist.replaceChildren();
        users.forEach(function (u) { mlist.appendChild(member_item(u, false, is_creator)); });
        _setup_manager_filters(mlist, mlist);
    }

    function open_ban_manager(blocked, not_banned, chan_name) {
        document.getElementById("manager_title").textContent  =
            chan_name ? "Ban users — " + chan_name : "Ban users";
        document.getElementById("col_left_title").textContent = "Blocked";
        document.getElementById("col_right_title").textContent = "Users";
        var right_col = document.getElementById("member_manager_right_col");
        if (right_col) right_col.classList.remove("hidden");
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

    // Read-only single-column view for users who can see but not manage
    // (any member of a private channel; anyone for a public channel's bans).
    function open_readonly_list(title, users, chan_name) {
        document.getElementById("manager_title").textContent  =
            chan_name ? title + " — " + chan_name : title;
        document.getElementById("col_left_title").textContent = title;
        var right_col = document.getElementById("member_manager_right_col");
        if (right_col) right_col.classList.add("hidden");
        var mgr = document.getElementById("member_manager");
        mgr.classList.remove("hidden");

        var mlist = document.getElementById("members_list");
        mlist.removeAttribute("ondrop");
        mlist.replaceChildren();
        users.forEach(function (u) { mlist.appendChild(member_item(u, false)); });
        _setup_manager_filters(mlist, mlist);
    }

    function member_item(user, draggable, mod_toggle) {
        var li = make("li", "member-item");
        li.draggable      = draggable !== false;
        li.dataset.uid    = user.id;
        li.dataset.peerId = user.peer_id || "";
        var av = user.avatar;
        if (av && av.startsWith("http")) av = _bp + "/proxy_img?url=" + encodeURIComponent(av);
        li.appendChild(avatar_img(av, null, user.name || user.display_name));
        var label = user.name || "?";
        if (user.peer_name) label += " @ " + user.peer_name;
        li.appendChild(make("span", null, label));

        if (mod_toggle) {
            var star = make("span", "mod-toggle", user.is_moderator ? "⭐" : "☆");
            star.title = user.is_moderator ? "Moderator — click to revoke" : "Click to make moderator";
            star.addEventListener("click", function (e) {
                e.stopPropagation();
                var next = !user.is_moderator;
                request("set_moderator", {
                    channel_id: current_channel_id, user_id: user.id, is_moderator: next,
                }).then(function (d) {
                    if (!d.ok) { show_error(d.reason); return; }
                    user.is_moderator = next;
                    star.textContent = next ? "⭐" : "☆";
                    star.title = next ? "Moderator — click to revoke" : "Click to make moderator";
                });
            });
            li.appendChild(star);
        } else if (user.is_moderator) {
            var badge = make("span", "mod-badge", "⭐");
            badge.title = "Moderator";
            li.appendChild(badge);
        }

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
            if (item) {
                if (!make_member) {
                    // Removing from the channel drops moderator status too — clear the stale UI.
                    var star = item.querySelector(".mod-toggle, .mod-badge");
                    if (star) star.remove();
                }
                if (dest) dest.appendChild(item);
            }
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
        td_ident.appendChild(avatar_img(u.avatar, null, u.display_name || u.name));
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

    function _copy_share_link(host_address, channel_id) {
        if (!host_address) return;
        var https_host = host_address.replace("wss://", "https://").replace("ws://", "http://");
        var url = https_host.replace(/\/$/, "") + "/join/" + channel_id;
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(url).then(function () {
                show_error("Link copied!");
            }).catch(function () { prompt("Copy this link:", url); });
        } else {
            prompt("Copy this link:", url);
        }
    }

    // ── Channels page ──────────────────────────────────────────────────
    function resolve_pasted_link(raw_url) {
        request("resolve_channel_link", { url: raw_url }).then(function (d) {
            if (!d.ok) { show_error(d.reason); return; }
            if (d.kind === "local") {
                open_channel(d.channel_id, null);
            } else if (d.kind === "remote") {
                open_channel(d.channel_id, d.peer_id);
            } else if (d.kind === "denied") {
                start_assisted_chat(d.owner.id, d.peer_id, d.owner.name,
                    "Hi! Could I get access to \"" + (d.channel_name || "this chat") + "\"?",
                    "🔒 You don't have access to \"" + (d.channel_name || "this chat") +
                    "\" — here's a draft message to ask " + d.owner.name + " for access. Review it and hit send.");
            } else if (d.kind === "not_connected") {
                if (d.is_owner) {
                    if (confirm("Connect to " + d.host + " to view this chat?")) {
                        request("add_peer", { address: d.host }).then(function (r) {
                            if (!r.ok) { show_error(r.reason); return; }
                            resolve_pasted_link(raw_url);
                        });
                    }
                } else {
                    // Save the link to the user's own Scratchpad rather than putting it in the
                    // admin DM — the admin only needs to know which host to connect to, not
                    // which private chat exists on it.
                    request("start_chat", { user_id: user_id, peer_id: null }).then(function (r) {
                        if (r.ok) request("message", { channel_id: r.channel_id, text: raw_url });
                    });
                    start_assisted_chat(d.owner_id, null, null,
                        "Could you connect to " + d.host + "? I'd like to access a chat there.",
                        "🔌 Your server isn't connected to " + d.host + " yet — only the owner can do that. " +
                        "I saved your chat link to your Scratchpad so you don't lose it — paste it back into " +
                        "Channels → 📤 Open link once they've connected. Review the draft below and hit send.");
                }
            }
        });
    }

    var _all_loaded_channels = []; // [{ch, host_address, peer_id}] — populated by setup_channels_page

    function _render_favorites_section() {
        var section = document.getElementById("favorites_section");
        var flist   = document.getElementById("favorites_list");
        if (!section || !flist) return;

        var favs = _all_loaded_channels.filter(function (entry) {
            return _favorite_channels.has(_fav_key(entry.ch.id, entry.peer_id || null));
        });

        section.classList.toggle("hidden", favs.length === 0);
        flist.replaceChildren();
        favs.forEach(function (entry) {
            var ch  = entry.ch;
            var key = _fav_key(ch.id, entry.peer_id || null);
            var li   = make("li", "fav-item");
            var icon = make("span", "icon", _channel_icon(ch));
            var link = make("a", null, ch.name);
            link.href = "#";
            if (ch.description) link.title = ch.description;
            link.addEventListener("click", function (e) {
                e.preventDefault();
                open_channel(ch.id, entry.peer_id || null);
            });
            li.appendChild(icon);
            li.appendChild(link);

            var trash = make("span", "trash", "✕");
            trash.title = "Remove from favorites";
            trash.addEventListener("click", function (e) {
                e.stopPropagation();
                _set_favorite(key, false);
            });
            li.appendChild(trash);

            flist.appendChild(li);
        });
    }

    function _fmt_search_date(iso) {
        var d = new Date(iso);
        var now = new Date();
        if (d.toDateString() === now.toDateString())
            return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        return d.toLocaleDateString([], { month: "short", day: "numeric" });
    }

    function _highlight_query(text, query) {
        var esc = text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
        var re = new RegExp(query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
        return esc.replace(re, function (m) { return "<mark>" + m + "</mark>"; });
    }

    function setup_channels_page() {
        var link_btn  = document.getElementById("open_link_btn");
        var link_form = document.getElementById("open_link_form");
        var link_input = document.getElementById("open_link_input");
        if (link_btn) {
            link_btn.addEventListener("click", function () {
                link_form.classList.toggle("hidden");
                if (!link_form.classList.contains("hidden")) link_input.focus();
            });
        }
        if (link_form) {
            link_form.addEventListener("submit", function (e) {
                e.preventDefault();
                var url = link_input.value.trim();
                if (!url) return;
                resolve_pasted_link(url);
                link_input.value = "";
                link_form.classList.add("hidden");
            });
        }

        var search_btn     = document.getElementById("search_btn");
        var search_overlay = document.getElementById("search_overlay");
        var close_search   = document.getElementById("close_search");
        var search_input   = document.getElementById("search_input");
        var search_results = document.getElementById("search_results");
        if (search_btn && search_overlay) {
            search_btn.addEventListener("click", function () {
                search_overlay.classList.remove("hidden");
                search_input.focus();
            });
        }
        if (close_search && search_overlay) {
            close_search.addEventListener("click", function () {
                search_overlay.classList.add("hidden");
            });
        }
        var _search_timer = null;
        if (search_input) {
            search_input.addEventListener("input", function () {
                clearTimeout(_search_timer);
                var q = search_input.value.trim();
                if (q.length < 2) { search_results.replaceChildren(); return; }
                _search_timer = setTimeout(function () {
                    request("search_messages", { query: q }).then(function (r) {
                        if (!r.ok) return;
                        var q2 = search_input.value.trim();
                        search_results.replaceChildren();
                        if (!r.results.length) {
                            search_results.appendChild(make("div", "search-empty", "No results found."));
                            return;
                        }
                        r.results.forEach(function (msg) {
                            var item = make("div", "search-result-item");
                            var meta = make("div", "search-result-meta");
                            meta.appendChild(make("span", "chat-icon", msg.channel_icon || "🗨️"));
                            meta.appendChild(make("span", "search-result-channel", msg.channel_name || "Channel"));
                            if (msg.peer_address) meta.appendChild(make("span", "search-result-peer", "@" + (msg.peer_address.replace(/wss?:\/\//, ""))));
                            meta.appendChild(make("span", "search-result-sep", "·"));
                            meta.appendChild(make("span", "", msg.sender_name || ""));
                            meta.appendChild(make("span", "search-result-time", _fmt_search_date(msg.created)));
                            var snippet = make("div", "search-result-snippet");
                            snippet.innerHTML = _highlight_query(msg.text || "", q2);
                            item.appendChild(meta);
                            item.appendChild(snippet);
                            item.addEventListener("click", function () {
                                search_overlay.classList.add("hidden");
                                open_channel(msg.channel_id, msg.peer_id || null);
                            });
                            search_results.appendChild(item);
                        });
                    });
                }, 300);
            });
        }

        var _own_host_address = null;

        request("read_channels").then(function (d) {
            if (!d.ok) return;
            _own_host_address = d.host_address;

            _all_loaded_channels = [];
            var list = document.getElementById("channels_list");
            list.replaceChildren();

            // Seed unread/mention counts from server-side read_state (local channels only).
            var mentions = 0;
            d.channels.forEach(function (ch) {
                var key = _chan_key(ch.id, null);
                if (ch.unread_count  > 0) _unread[key] = ch.unread_count;
                if (ch.mention_count > 0) { _channel_mentions[key] = ch.mention_count; mentions += ch.mention_count; }
            });
            _mention_count = mentions;
            _update_unread_badge();
            _update_mention_badge();

            d.channels.forEach(function (ch) {
                _all_loaded_channels.push({ ch: ch, host_address: d.host_address, peer_id: null });
                list.appendChild(build_channel_item(list, ch, d.host_address));
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
                        var vis  = make("span", "channel-visibility", ch.public ? "🌐" : "🔒");
                        vis.title = ch.public ? "Public channel" : "Private channel";
                        var link = make("a", null, ch.name);
                        link.href = "#";
                        link.addEventListener("click", function (e) {
                            e.preventDefault();
                            open_channel(ch.id, peer.id);
                        });
                        li.appendChild(icon);
                        li.appendChild(vis);
                        li.appendChild(link);

                        _all_loaded_channels.push({ ch: ch, host_address: peer.address, peer_id: peer.id });

                        var rfkey    = _fav_key(ch.id, peer.id);
                        var rfav_btn = make("span", "stream-toggle", _favorite_channels.has(rfkey) ? "⭐" : "☆");
                        rfav_btn.title = _favorite_channels.has(rfkey) ? "Remove from favorites" : "Add to favorites";
                        rfav_btn.addEventListener("click", function (e) {
                            e.stopPropagation();
                            _set_favorite(rfkey, !_favorite_channels.has(rfkey));
                        });
                        _register_fav_button(rfkey, rfav_btn);
                        li.appendChild(rfav_btn);

                        var share = make("span", "stream-toggle", "📤");
                        share.title = "Copy link to this chat";
                        share.addEventListener("click", function (e) {
                            e.stopPropagation();
                            _copy_share_link(peer.address, ch.id);
                        });
                        li.appendChild(share);

                        var mkey  = (ch.id || "") + "|" + (peer.id || "");
                        // all remote channels: opt-in via _stream_subscribed
                        var muted = !_stream_subscribed.has(mkey);
                        var mbtn  = make("span", "stream-toggle", muted ? "🔕" : "🔔");
                        mbtn.title = muted ? "Add to stream" : "Remove from stream";
                        mbtn.addEventListener("click", function (e) {
                            e.stopPropagation();
                            muted = !muted;
                            if (muted) { _stream_subscribed.delete(mkey); }
                            else       { _stream_subscribed.add(mkey); }
                            _save_stream_subscribed();
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
            _render_favorites_section();
        });

        document.getElementById("new_channel_btn").addEventListener("click", function () {
            open_channel_settings(null, function (channel) {
                _all_loaded_channels.push({ ch: channel, host_address: _own_host_address, peer_id: null });
                var list = document.getElementById("channels_list");
                list.appendChild(build_channel_item(list, channel, _own_host_address));
            });
        });
    }

    function build_channel_item(list, ch, host_address) {
        var is_mine = ch.created_by === user_id;
        var li   = make("li", is_mine ? "own-channel" : "");
        var icon = make("span", "icon", _channel_icon(ch));
        var vis  = make("span", "channel-visibility", ch.public ? "🌐" : "🔒");
        vis.title = ch.public ? "Public channel" : "Private channel";
        var link = make("a", null, ch.name);
        link.href = "#";
        if (ch.description) link.title = ch.description;
        var poster_el = make("span", "channel-poster-count");
        if (ch.poster_count) poster_el.textContent = ch.poster_count;
        link.addEventListener("click", function (e) {
            e.preventDefault();
            open_channel(ch.id, null);
        });
        li.appendChild(icon);
        li.appendChild(vis);
        li.appendChild(link);
        li.appendChild(poster_el);

        // Fixed-width slots so the same icon always lands in the same
        // column across rows, even when a given row doesn't have it.
        var actions = make("span", "channel-actions");

        var creator_label = is_mine ? "jij" : (ch.created_by_name || "");
        var creator_el = make("span", "channel-creator", creator_label);
        actions.appendChild(creator_el);

        var fkey    = _fav_key(ch.id, null);
        var fav_btn = make("span", "stream-toggle action-slot", _favorite_channels.has(fkey) ? "⭐" : "☆");
        fav_btn.title = _favorite_channels.has(fkey) ? "Remove from favorites" : "Add to favorites";
        fav_btn.addEventListener("click", function () { _set_favorite(fkey, !_favorite_channels.has(fkey)); });
        _register_fav_button(fkey, fav_btn);
        actions.appendChild(fav_btn);

        var share = make("span", "stream-toggle action-slot", "📤");
        share.title = "Copy link to this chat";
        share.addEventListener("click", function () { _copy_share_link(host_address, ch.id); });
        actions.appendChild(share);

        var excluded = !!ch.stream_excluded;
        var stream_btn = make("span", "stream-toggle action-slot", excluded ? "🔕" : "🔔");
        stream_btn.title = excluded ? "Add to stream" : "Remove from stream";
        stream_btn.addEventListener("click", function () {
            request("toggle_stream_channel", { channel_id: ch.id }).then(function (d) {
                if (!d.ok) { show_error(d.reason); return; }
                excluded = d.stream_excluded;
                stream_btn.textContent = excluded ? "🔕" : "🔔";
                stream_btn.title = excluded ? "Add to stream" : "Remove from stream";
            });
        });
        actions.appendChild(stream_btn);

        var edit_btn = make("span", "stream-toggle action-slot");
        if (ch.can_manage) {
            edit_btn.textContent = "✏️";
            edit_btn.title = "Edit channel";
            edit_btn.addEventListener("click", function (e) {
                e.stopPropagation();
                open_channel_settings(ch, function (updated) {
                    ch.icon             = updated.icon;
                    ch.public           = updated.public;
                    ch.allow_replies    = updated.allow_replies;
                    ch.post_restricted  = updated.post_restricted;
                    ch.restrict_replies = updated.restrict_replies;
                    ch.allow_images     = updated.allow_images;
                    ch.allow_reactions  = updated.allow_reactions;
                    ch.edit_mode        = updated.edit_mode;
                    ch.description      = updated.description;
                    icon.textContent = _channel_icon(ch);
                    vis.textContent  = ch.public ? "🌐" : "🔒";
                    vis.title        = ch.public ? "Public channel" : "Private channel";
                    link.title       = ch.description || "";
                });
            });
        }
        actions.appendChild(edit_btn);

        // Deletion is destructive enough to stay creator-only — no moderator
        // or server-owner override, matching the server-side check.
        var trash = make("span", "trash action-slot");
        if (ch.created_by === user_id) {
            trash.textContent = "✕";
            trash.addEventListener("click", function () {
                if (!confirm("Delete channel " + ch.name + "?")) return;
                request("delete_channel", { id: ch.id }).then(function (d) {
                    if (d.ok) li.remove();
                    else show_error(d.reason);
                });
            });
        }
        actions.appendChild(trash);

        li.appendChild(actions);
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
                            (m.channel_icon || (m.channel_public ? "🌐" : "🔒")) + " " + m.channel_name));
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

            var message_section = document.getElementById("message_actions");
            if (message_section) {
                message_section.classList.remove("hidden");
                document.getElementById("profile_message_btn").addEventListener("click", function () {
                    start_assisted_chat(target.id, target.peer_id || null, target.name, null);
                });
            }

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
