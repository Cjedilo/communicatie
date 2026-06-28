var communicatie = (function () {

    // ── State ──────────────────────────────────────────────────────────
    var socket;
    var user_id   = "";
    var user_name = "";
    var is_owner  = false;

    var current_channel_id   = null;
    var current_channel_peer = null;
    var pending_parent_id    = null;
    var pending_image        = null;

    var known_profiles  = {};   // id → {name, avatar}
    var needed_profiles = new Set();

    // ── WebSocket ──────────────────────────────────────────────────────
    var _queue = [];   // messages queued before socket is open

    function connect() {
        socket = new WebSocket("wss://" + location.host + "/ws");

        socket.onopen = function () {
            _queue.forEach(function (msg) { socket.send(msg); });
            _queue = [];
        };

        socket.onerror = function () {};

        socket.onmessage = function (event) {
            var data = JSON.parse(event.data);
            dispatch(data);
        };

        socket.onclose = function () {
            setTimeout(connect, 3000);
        };
    }

    function send(type, params) {
        var msg = JSON.stringify(Object.assign({ type: type }, params || {}));
        if (socket && socket.readyState === WebSocket.OPEN) {
            socket.send(msg);
        } else {
            _queue.push(msg);
        }
    }

    // Each send returns a Promise that resolves when the matching response arrives.
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
        var type      = data.type;
        var resolvers = _pending[type];
        if (resolvers && resolvers.length) {
            resolvers.shift()(data);
        }
        if (type === "message" && data.ok) {
            on_live_message(data.message);
        }
    }

    // ── Session expired → login ────────────────────────────────────────
    function handle_auth_error() {
        show_page("/login.html", setup_login_page);
    }

    // ── Page loading ───────────────────────────────────────────────────
    var _page_setup = {
        "/channels.html": function () { setup_channels_page(); },
        "/user.html":     function () { setup_user_page(); },
    };

    function show_page(url, callback) {
        fetch(url)
            .then(function (r) { return r.text(); })
            .then(function (html) {
                // Safe: templates contain only static HTML, no user data.
                // All user content is inserted via text nodes (never innerHTML).
                document.getElementById("main").innerHTML = html;
                var setup = _page_setup[url];
                if (setup)    setup();
                if (callback) callback();
            })
            .catch(function () { show_error("Could not load page"); });
    }

    function show_error(msg) {
        var el = document.getElementById("error");
        el.textContent = msg;
        el.classList.remove("hidden");
        setTimeout(function () { el.classList.add("hidden"); }, 5000);
    }

    // ── Safe DOM helpers ───────────────────────────────────────────────
    function text_node(str) {
        return document.createTextNode(str || "");
    }

    function make(tag, cls, content) {
        var el = document.createElement(tag);
        if (cls)     el.className = cls;
        if (content) el.appendChild(text_node(content));
        return el;
    }

    function avatar_img(src, cls) {
        var img = document.createElement("img");
        img.className = cls || "avatar";
        img.src       = src ? (src.startsWith("http") ? src : "/img/" + src) : "";
        img.alt       = "";
        return img;
    }

    function fmt_time(iso) {
        if (!iso) return "";
        var d = new Date(iso);
        return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    }

    // ── Init ───────────────────────────────────────────────────────────
    function init(logged_in, uid, uname, owner) {
        user_id   = uid;
        user_name = uname;
        is_owner  = owner;

        if (logged_in) {
            connect();
            show_page("/channels.html");
        } else {
            show_page("/login.html", setup_login_page);
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

        fetch("/login", {
            method:  "POST",
            headers: { "Content-Type": "application/json" },
            body:    JSON.stringify({ name: name, password: password }),
        })
            .then(function (r) { return r.json(); })
            .then(function (d) {
                if (d.ok) {
                    location.reload();
                } else {
                    show_error(d.reason || "Login failed");
                }
            })
            .catch(function () { show_error("Login error"); });
    }

    function do_register() {
        var name     = document.getElementById("register_name").value.trim();
        var password = document.getElementById("register_password").value;
        if (!name || !password) return;

        fetch("/register", {
            method:  "POST",
            headers: { "Content-Type": "application/json" },
            body:    JSON.stringify({ name: name, password: password }),
        })
            .then(function (r) { return r.json(); })
            .then(function (d) {
                if (d.ok) {
                    document.getElementById("login_name").value     = name;
                    document.getElementById("login_password").value = password;
                    show_error("Account created — logging you in…");
                    do_login();
                } else {
                    show_error(d.reason || "Registration failed");
                }
            })
            .catch(function () { show_error("Registration error"); });
    }

    // ── Channels page ──────────────────────────────────────────────────
    function setup_channels_page() {
        request("read_channels").then(function (d) {
            if (!d.ok) return;
            var list = document.getElementById("channels_list");
            list.replaceChildren();
            d.channels.forEach(function (ch) { add_channel(list, ch); });

            if (d.peers) {
                var plist = document.getElementById("peers_list");
                plist.replaceChildren();
                d.peers.forEach(function (p) { add_peer_item(plist, p); });
            }
        });

        request("read_users").then(function (d) {
            if (!d.ok) return;
            var list = document.getElementById("users_list");
            list.replaceChildren();
            d.users.forEach(function (u) {
                var li   = make("li");
                var img  = avatar_img(u.avatar);
                var name = make("span", null, u.name);
                li.appendChild(img);
                li.appendChild(name);

                if (is_owner) {
                    var trash = make("span", "trash", "✕");
                    trash.addEventListener("click", function () { delete_user(u.id); });
                    li.appendChild(trash);
                }
                list.appendChild(li);
            });
        });

        function refresh_peer_display() {
            request("read_peers").then(function (d) {
                if (!d.ok) return;
                if (d.peer_address) {
                    var display = document.getElementById("peer_address_display");
                    if (!display) return;
                    display.classList.remove("hidden");
                    document.getElementById("peer_address_value").textContent = d.peer_address;
                }
                if (is_owner) {
                    if (d.peer_name)    document.getElementById("peer_name_input").value        = d.peer_name;
                    if (d.peer_address) document.getElementById("peer_address_self_input").value = d.peer_address;
                }
            });
        }

        request("read_peers").then(function (d) {
            if (!d.ok) return;

            // Show this server's address to everyone (read-only)
            if (d.peer_address) {
                var display = document.getElementById("peer_address_display");
                display.classList.remove("hidden");
                document.getElementById("peer_address_value").textContent = d.peer_address;
                document.getElementById("peer_address_copy").addEventListener("click", function () {
                    navigator.clipboard.writeText(d.peer_address).then(function () {
                        document.getElementById("peer_address_copy").textContent = "Copied!";
                        setTimeout(function () {
                            document.getElementById("peer_address_copy").textContent = "Copy";
                        }, 2000);
                    });
                });
            }

            if (is_owner) {
                document.getElementById("peer_admin").classList.remove("hidden");
                document.getElementById("peer_admin_address").classList.remove("hidden");

                // Pre-fill current values
                if (d.peer_name)    document.getElementById("peer_name_input").value        = d.peer_name;
                if (d.peer_address) document.getElementById("peer_address_self_input").value = d.peer_address;

                document.getElementById("add_peer_form").addEventListener("submit", function (e) {
                    e.preventDefault();
                    var addr = document.getElementById("peer_address_input").value.trim();
                    if (addr) add_peer(addr);
                });

                document.getElementById("peer_name_form").addEventListener("submit", function (e) {
                    e.preventDefault();
                    var name = document.getElementById("peer_name_input").value.trim();
                    if (!name) return;
                    request("set_peer_name", { name: name }).then(function (d) {
                        if (d.ok) refresh_peer_display();
                        else show_error(d.reason);
                    });
                });

                document.getElementById("peer_address_form").addEventListener("submit", function (e) {
                    e.preventDefault();
                    var addr = document.getElementById("peer_address_self_input").value.trim();
                    if (!addr) return;
                    request("set_peer_address", { address: addr }).then(function (d) {
                        if (d.ok) refresh_peer_display();
                        else show_error(d.reason);
                    });
                });
            }
        });

        document.getElementById("create_channel_form").addEventListener("submit", function (e) {
            e.preventDefault();
            create_channel();
        });
    }

    function add_channel(list, ch) {
        var li    = make("li");
        var lock  = make("span", "icon", ch.public ? "🌐" : "🔒");
        var link  = make("a", null, ch.name);
        link.href = "#";
        link.addEventListener("click", function (e) {
            e.preventDefault();
            open_channel(ch.id, null);
        });

        li.appendChild(lock);
        li.appendChild(link);

        var trash = make("span", "trash", "✕");
        trash.addEventListener("click", function () { delete_channel(ch.id); });
        li.appendChild(trash);
        list.appendChild(li);
    }

    function add_peer_item(plist, peer) {
        var header   = make("li", "peer-header");
        var label    = peer.name || peer.address;
        var name_el  = make("span", "peer-name", label);
        if (peer.name && peer.address) name_el.title = peer.address;
        header.appendChild(name_el);
        plist.appendChild(header);

        if (peer.channels && peer.channels.length) {
            peer.channels.forEach(function (ch) {
                var li   = make("li", "peer-channel");
                var icon = make("span", "icon", ch.public ? "🌐" : "🔒");
                var link = make("a", null, ch.name);
                link.href = "#";
                link.addEventListener("click", function (e) {
                    e.preventDefault();
                    open_channel(ch.id, peer.id);
                });
                li.appendChild(icon);
                li.appendChild(link);
                plist.appendChild(li);
            });
        } else {
            plist.appendChild(make("li", "peer-empty", "No public channels"));
        }
    }

    function create_channel() {
        var name    = document.getElementById("channel_name").value.trim();
        var private = document.getElementById("channel_private").checked;
        if (!name) return;

        request("create_channel", { name: name, public: !private }).then(function (d) {
            if (d.ok) {
                document.getElementById("channel_name").value = "";
                document.getElementById("channel_private").checked = false;
                var list = document.getElementById("channels_list");
                if (list) add_channel(list, d.channel);
            } else {
                show_error(d.reason || "Could not create channel");
            }
        });
    }

    function delete_channel(id) {
        if (!confirm("Delete this channel?")) return;
        request("delete_channel", { id: id }).then(function (d) {
            if (!d.ok) show_error(d.reason || "Could not delete channel");
            else setup_channels_page();
        });
    }

    function delete_user(id) {
        if (!confirm("Delete this user?")) return;
        request("delete_user", { id: id }).then(function (d) {
            if (!d.ok) show_error(d.reason || "Could not delete user");
            else setup_channels_page();
        });
    }

    function add_peer(address) {
        request("add_peer", { address: address }).then(function (d) {
            if (!d.ok) { show_error(d.reason || "Could not connect to peer"); return; }
            document.getElementById("peer_address_input").value = "";
            var plist = document.getElementById("peers_list");
            add_peer_item(plist, d.peer);
        });
    }

    // ── Chat page ──────────────────────────────────────────────────────
    function open_channel(channel_id, peer_id) {
        current_channel_id   = channel_id;
        current_channel_peer = peer_id || null;
        pending_parent_id    = null;
        pending_image        = null;

        send("unsubscribe_all");

        show_page("/chat.html", function () {
            setup_chat_page();
            load_channel();
        });
    }

    function setup_chat_page() {
        document.getElementById("send_btn").addEventListener("click", send_message);
        document.getElementById("message_input").addEventListener("keydown", function (e) {
            if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                send_message();
            }
        });

        document.getElementById("drop_zone").addEventListener("dragover", function (e) {
            e.preventDefault();
            this.classList.add("active");
        });
        document.getElementById("drop_zone").addEventListener("dragleave", function () {
            this.classList.remove("active");
        });

        document.getElementById("close_member_manager").addEventListener("click", function () {
            document.getElementById("member_manager").classList.add("hidden");
        });
    }

    function load_channel() {
        request("read_channel", {
            id:      current_channel_id,
            peer_id: current_channel_peer,
        }).then(function (d) {
            if (!d.ok) { show_error(d.reason); return; }

            var container = document.getElementById("messages");
            container.replaceChildren();

            d.messages.forEach(function (msg) { append_message(container, msg); });
            container.scrollTop = container.scrollHeight;

            request_profiles();

            if (!d.remote) {
                subscribe_for_members();
            }
        });
    }

    function subscribe_for_members() {
        var manager_btn = make("button", "secondary", "Members");
        manager_btn.addEventListener("click", function () {
            request("read_members", { channel_id: current_channel_id }).then(function (d) {
                if (!d.ok) { show_error(d.reason || "Could not load members"); return; }
                open_member_manager(d.members, d.non_members);
            });
        });
        document.getElementById("input_area").prepend(manager_btn);
    }

    function reply_depth(container, parent_id, depth) {
        depth = depth || 1;
        var el = container.querySelector("[data-id='" + parent_id + "']");
        if (!el || !el.dataset.parentId) return depth;
        return reply_depth(container, el.dataset.parentId, depth + 1);
    }

    function append_message(container, msg) {
        var is_reply = !!msg.parent_id;
        var div = make("div", "message" + (is_reply ? " reply" : ""));
        div.dataset.id = msg.id;
        if (is_reply) div.dataset.parentId = msg.parent_id;

        var img = avatar_img(msg.sender_avatar || (known_profiles[msg.sender_user_id] || {}).avatar);
        img.classList.add("avatar");
        if (msg.sender_user_id && !known_profiles[msg.sender_user_id]) {
            needed_profiles.add(msg.sender_user_id);
        }

        var body   = make("div", "body");
        var header = make("div", "header");

        var is_remote  = !!msg.sender_peer_id;
        var sender_name = msg.sender_name
            || (known_profiles[msg.sender_user_id] || {}).name
            || (is_remote ? "@ " + (msg.peer_name || msg.peer_address || "?") : "…");
        var sender = make("span", "sender" + (is_remote ? " remote" : ""), sender_name);
        sender.dataset.uid = msg.sender_user_id;

        var time = make("span", "time", fmt_time(msg.created));
        header.appendChild(sender);
        header.appendChild(time);

        if (msg.remote && msg.peer_name) {
            var peer_tag = make("span", "time", " @ " + msg.peer_name);
            header.appendChild(peer_tag);
        }

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

                var insert_after = parent_el;
                var next = insert_after.nextElementSibling;
                while (next && next.dataset.parentId === msg.parent_id) {
                    insert_after = next;
                    next = insert_after.nextElementSibling;
                }
                insert_after.insertAdjacentElement("afterend", div);
                return;
            }
        }

        container.appendChild(div);
    }

    function render_content(body, msg) {
        if (msg.text) {
            body.appendChild(make("p", "text", msg.text));
        }
        if (msg.image) {
            var img = document.createElement("img");
            img.className = "attach";
            img.src = (msg.image.startsWith("http") || msg.image.startsWith("/")) ? msg.image : "/img/" + msg.image;
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
                var peer_name = msg.peer_name || "their server";
                var notice    = make("span", "unavailable",
                    "Unavailable — contact the owner of " + peer_name);
                body.appendChild(notice);
            }
        });
    }

    function start_reply(msg) {
        pending_parent_id = msg.id;
        var ctx = document.getElementById("reply_context");
        ctx.classList.remove("hidden");
        ctx.replaceChildren();

        var label = make("span", null,
            "Replying to " + (msg.sender_name || "…") + ": " + (msg.text || "").slice(0, 40));
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

        var file = event.dataTransfer.files[0];
        if (!file) return;

        var form = new FormData();
        form.append("image", file);

        fetch("/upload", { method: "POST", body: form })
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
                img.src = "/img/" + d.image;
                img.alt = "";
                preview.appendChild(img);
            })
            .catch(function () { show_error("Image upload failed"); });
    }

    // ── Member manager ─────────────────────────────────────────────────
    function open_member_manager(members, non_members) {
        var mgr = document.getElementById("member_manager");
        mgr.classList.remove("hidden");

        var mlist  = document.getElementById("members_list");
        var nmlist = document.getElementById("non_members_list");
        mlist.replaceChildren();
        nmlist.replaceChildren();

        members.forEach(function (u) {
            mlist.appendChild(member_item(u));
        });
        non_members.forEach(function (u) {
            nmlist.appendChild(member_item(u));
        });
    }

    function member_item(user) {
        var li  = make("li", "member-item");
        li.draggable      = true;
        li.dataset.uid    = user.id;
        li.dataset.peerId = user.peer_id || "";
        li.appendChild(avatar_img(user.avatar));
        var label = user.name;
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

    // ── User page ──────────────────────────────────────────────────────
    function setup_user_page() {
        request("read_user", { id: user_id }).then(function (d) {
            if (!d.ok) return;
            var u = d.user;
            document.getElementById("user_name_display").textContent = u.name;
            document.getElementById("user_joined").textContent =
                "Joined " + new Date(u.created).toLocaleDateString();
            var avatar_el = document.getElementById("user_avatar");
            if (u.avatar) avatar_el.src = "/img/" + u.avatar;
        });

        document.getElementById("avatar_input").addEventListener("change", function () {
            var file = this.files[0];
            if (!file) return;
            var form = new FormData();
            form.append("avatar", file);
            fetch("/set_avatar", { method: "POST", body: form })
                .then(function (r) {
                    if (r.status === 401) { handle_auth_error(); return null; }
                    return r.json();
                })
                .then(function (d) {
                    if (d && d.avatar) {
                        document.getElementById("user_avatar").src = "/img/" + d.avatar;
                    }
                })
                .catch(function () { show_error("Avatar upload failed"); });
        });

        document.getElementById("delete_account_btn").addEventListener("click", function () {
            if (!confirm("Delete your account? This cannot be undone.")) return;
            request("delete_user", { id: user_id }).then(function (d) {
                if (d.ok) location.href = "/logout";
                else show_error(d.reason || "Could not delete account");
            });
        });
    }

    // ── Profile cache ──────────────────────────────────────────────────
    function request_profiles() {
        if (!needed_profiles.size) return;
        var ids = Array.from(needed_profiles);
        needed_profiles.clear();

        request("read_profiles", { ids: ids }).then(function (d) {
            if (!d.ok) return;
            d.users.forEach(function (u) {
                known_profiles[u.id] = u;
            });
            // Update any placeholder names/avatars in the DOM
            ids.forEach(function (id) {
                var profile = known_profiles[id];
                if (!profile) return;
                document.querySelectorAll("[data-uid='" + id + "']").forEach(function (el) {
                    if (el.classList.contains("sender")) el.textContent = profile.name;
                });
            });
        });
    }

    // ── Public API ─────────────────────────────────────────────────────
    return {
        init:           init,
        show_page:      show_page,
        open_channel:   open_channel,
        drop_image:     drop_image,
        drop_member:    drop_member,
        setup_user_page: setup_user_page,
    };

})();
