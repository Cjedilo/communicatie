var communicatie = (function(){

    socket_open_todos = [];
    socket = new WebSocket("wss://www.appelo.nl:8181");
    socket.onopen = function(_event) {
        console.log("socket open");
        for(todo in socket_open_todos) {
            socket_open_todos[todo]();
        }
        socket_open_todos = [];
    };
    socket.onerror = function(event) {
        console.error('onerror!!!', event);
    }
    socket.onmessage = function(event) {
        console.log(`[message] Data received from server: ${event.data}`);
        data = JSON.parse(event.data);
        if(data.channels) {
            list = document.getElementById("channels");
            for (const channel in data.channels) {
                item = document.createElement("li");
                item.dataset.id = data.channels[channel].id;
                link = document.createElement("a")
                link.setAttribute("href", `chat.html?id=${data.channels[channel].id}`);
                link.appendChild(document.createTextNode(data.channels[channel].name));
                span = document.createElement("span");
                span.setAttribute("class", "trash");
                span.setAttribute("onclick", `communicatie.trash_channel('${data.channels[channel].id}')`);
                span.innerHTML = "&#x1F5D1;";
                item.appendChild(link);
                item.appendChild(span);
                list.appendChild(item);
            }
        } else if(data.users) {
            list = document.getElementById("users");
            for (const user in data.users) {
                item = document.createElement("li");
                item.dataset.id = data.users[user].id;
                link = document.createElement("a")
                link.setAttribute("href", `user.html?id=${data.users[user].id}`);
                link.appendChild(document.createTextNode(data.users[user].name));
                span = document.createElement("span");
                span.setAttribute("class", "trash");
                span.setAttribute("onclick", `communicatie.trash_user('${data.users[user].id}')`);
                span.innerHTML = "&#x1F5D1;";
                item.appendChild(link);
                item.appendChild(span);
                list.appendChild(item);
            }
        } else if(data.delete_channel) {
            list = document.getElementById("channels");
            for(item in list.children) {
                if(list.children[item].dataset.id === data.delete_channel) {
                    list.children[item].remove();
                    break;
                }
            }
        } else if(data.chat_name) {
            header = document.getElementById("channel_name");
            header.textContent = data.chat_name;
            if(data.messages) {
                messages = document.getElementById("messages");
                for(message in data.messages) {
                    div = document.createElement("div");
                    div.setAttribute("class", "message");
                    div.appendChild(document.createTextNode(data.messages[message]));
                    messages.appendChild(div);
                }
            }
        } else if(data.message) {
            messages = document.getElementById("messages");
            div = document.createElement("div");
            div.setAttribute("class", "message");
            div.appendChild(document.createTextNode(data.message));
            messages.appendChild(div);
        } else if (data.user_id) {
            document.cookie = `user_id=${data.user_id}`;
            document.location.replace("index.html");
        }
    };    

    function new_channel() {
        let channel_name = document.getElementById('channel_name').value;
        socket.send(JSON.stringify({
            request: "new_channel",
            channel_name: channel_name,
        }));
    }

    function new_user() {
        let user_name = document.getElementById('new_user').value;
        let password = document.getElementById('password').value;
        socket.send(JSON.stringify({
            request: "new_user",
            user_name: user_name,
            password: password,
        }));
    }

    function trash_channel(id) {
        socket.send(JSON.stringify({
            delete: "channel",
            id: id
        }));
    }

    function trash_user(id) {
        socket.send(JSON.stringify({
            delete: "user",
            id: id
        }));
    }

    function init_index() {
        console.log("init");
        if(document.cookie.startsWith("user_id=")) {
            user_id = document.cookie.slice(-36);
        } else {
            document.location.replace("login.html");
        }
        if(socket.readyState == socket.OPEN) {
            socket.send('{"request": "channels"}');
            socket.send('{"request": "users"}');
        } else {
            socket_open_todos.push(init_index);
        }
    }

    function init_chat() {
        console.log("init");
        if(document.cookie.startsWith("user_id=")) {
            user_id = document.cookie.slice(-36);
        } else {
            document.location.replace("login.html");
        }
        if(socket.readyState == socket.OPEN) {
            const searchParams = new URLSearchParams(window.location.search);
            if(searchParams.has('id')) {
                channel_id = searchParams.get('id');
                socket.send(JSON.stringify({
                    channel: channel_id
                }))
            } else {
                channel_id = null;
            }
        } else {
            socket_open_todos.push(init_chat);
        }
    }

    function add_message() {
        message = document.getElementById("message").value;
        socket.send(JSON.stringify({
            "message": message,
            "channel": channel_id,
            "user": user_id,
        }));

        document.getElementById("message").value = "";
    }

    function login() {
        user_name = document.getElementById("user_name").value;
        password = document.getElementById("password").value;
        socket.send(JSON.stringify({
            "login": user_name,
            "password": password,
        }))
    }

    function logout() {
        document.cookie = "";
        document.location.replace("login.html");
    }

    return {
        init_index: init_index,
        init_chat: init_chat,
        new_channel: new_channel,
        new_user: new_user,
        trash_channel: trash_channel,
        trash_user: trash_user,
        add_message: add_message,
        login: login,
        logout: logout,
    };
})();