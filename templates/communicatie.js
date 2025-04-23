var communicatie = (function(){
    needed_profiles = new Set();
    known_profiles = {};
    socket_open_todos = [];
    public_id = private_id = null;
    socket = new WebSocket("wss://www.appelo.nl:8181/ws");
    
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

    function add_channel(list, channel) {
        item = document.createElement("li");
        item.dataset.id = channel.id;
        link = document.createElement("a");
        link.setAttribute("href", `chat.html?id=${channel.id}`);
        link.appendChild(document.createTextNode(channel.name));
        item.appendChild(link);
        if(channel.owner === public_id) {
            span = document.createElement("span");
            span.setAttribute("class", "trash");
            span.setAttribute("onclick", `communicatie.delete_channel('${channel.id}')`);
            span.innerHTML = "&#x1F5D1;";
            item.appendChild(span);
        }
        list.appendChild(item);
    }

    function request_user_profiles() {
        if(needed_profiles.size !== 0) {
            request("read_profiles", Array.from(needed_profiles));
            needed_profiles.clear();
        }
    }

    function create_message_header(message) {
        message_header = document.createElement("div");
        message_header.setAttribute("class", "message_header");
        span = document.createElement("span");
        span.setAttribute("class", "message_sender");
        span.dataset.id = message.send_by;
        if(known_profiles[message.send_by]) {
            span.appendChild(document.createTextNode(known_profiles[message.send_by].name));
        } else {
            needed_profiles.add(message.send_by);
            span.appendChild(document.createTextNode("retrieving..."));
        }
        message_header.appendChild(span);
        span = document.createElement("span");
        span.setAttribute("class", "message_date");
        span.appendChild(document.createTextNode("@" +  message.date));
        message_header.appendChild(span);

        return message_header;
    }

    function append_message(messages, message) {
        div = document.createElement("div");
        div.dataset.id = message.id;
        div.setAttribute("class", "message");

        div.appendChild(create_message_header(message));

        inner_div = document.createElement("div");
        inner_div.setAttribute("class", "message_text");
        
        img = document.createElement("img");
        img.dataset.id = message.send_by;
        if(known_profiles[message.send_by]) {
            img.setAttribute("src", known_profiles[message.send_by].avatar);
        } else {
            img.setAttribute("src", "img/unknown.png");
            needed_profiles.add(message.send_by);
        }
        inner_div.appendChild(img);
        
        message_div = document.createElement("div");
        if(message.image) {
            img = document.createElement("img");
            img.src = message.image
            img.classList.add("message_image");
            message_div.appendChild(img);
            message_div.appendChild(document.createElement("br"));
        }
        message_div.appendChild(document.createTextNode(message.message));
        inner_div.appendChild(message_div);
        reply = document.createElement("span");
        reply.setAttribute("class", "icons");
        reply.setAttribute("onclick", "communicatie.reply(this.parentNode.parentNode)");
        reply.innerHTML = "&#x21b5;";
        inner_div.appendChild(reply);

        div.appendChild(inner_div);
        messages.appendChild(div);
        
        request("read_channel", {
            id: message.id,
        });

    }

    function reply(element) {
        console.log(element);
        reply = document.getElementById("reply");
        if(reply) {
            reply.remove();
        }
        parent = document.querySelector(`[data-id*="${element.dataset.id}"]`);
        div = document.createElement("div");
        div.id = "reply";
        img = document.createElement("img");
        img.id = `image-${element.dataset.id}`;
        img.hidden = true;
        img.classList.add("message_image");
        div.appendChild(img);
        text = document.createElement("textarea");
        text.setAttribute("ondrop", `communicatie.image(event, '${element.dataset.id}')`);
        div.appendChild(text);
        send = document.createElement("button");
        send.setAttribute("onclick", `communicatie.add_message('${element.dataset.id}', this.previousElementSibling.value)`)
        send.setAttribute("class", "send");
        send.appendChild(document.createTextNode("send"));
        div.appendChild(send);
        cancel = document.createElement("button");
        cancel.setAttribute("onclick", "this.parentNode.remove()")
        cancel.setAttribute("class", "cancel");
        cancel.appendChild(document.createTextNode("cancel"));
        div.appendChild(cancel);
        parent.appendChild(div);
    }

    socket.onmessage = function(event) {
        console.log(`[message] Data received from server: ${event.data}`);
        data = JSON.parse(event.data);
        switch(data.response) {
            case "login":
                if(data.value.private_id) {
                    document.cookie = `user_id=${data.value.private_id}:${data.value.public_id}`;
                    document.location.replace("index.html");
                }
 
                break;
            case "read_channels":
                list = document.getElementById("channels");
                for (const channel in data.value) {
                    add_channel(list, data.value[channel]);
                }

                break;
            case "read_users":
                list = document.getElementById("users");
                for (const user in data.value) {
                    item = document.createElement("li");
                    item.dataset.id = data.value[user].id;
                    link = document.createElement("a")
                    link.setAttribute("href", `user.html?id=${data.value[user].id}`);
                    link.appendChild(document.createTextNode(data.value[user].name));
                    item.appendChild(link);
                    if(data.value[user].id === public_id) {
                        span = document.createElement("span");
                        span.setAttribute("class", "trash");
                        span.setAttribute("onclick", `communicatie.delete_user('${data.value[user].id}')`);
                        span.innerHTML = "&#x1F5D1;";
                        item.appendChild(span);
                    }
                    list.appendChild(item);
                }

                break;
            case "read_channel":
                if(data.value.channel_name) {
                    header = document.getElementById("channel_name");
                    header.textContent = data.value.channel_name;
                    parent = document.getElementById("messages");
                } else {
                    parent = document.querySelector(`[data-id*="${data.value.parent}"]`);
                }

                for(message in data.value.messages) {
                    append_message(parent, data.value.messages[message]);
                }
                request_user_profiles();
                break;
            case "message":
                if(data.value.parent == communicatie.get_channel_id()) {
                    parent = document.getElementById("messages");
                } else {
                    parent = document.querySelector(`[data-id*="${data.value.parent}"]`);
                }

                append_message(parent, data.value);
                request_user_profiles();

                break;
            case "create_channel":
                list = document.getElementById("channels");
                add_channel(list, data.value);

                break;
            case "delete_channel":
                list = document.getElementById("channels");
                for(item in list.children) {
                    if(list.children[item].dataset.id === data.value) {
                        list.children[item].remove();
                        break;
                    }
                }
                
                break;
            case "delete_user":
                    list = document.getElementById("users");
                    for(item in list.children) {
                        if(list.children[item].dataset.id === data.value) {
                            list.children[item].remove();
                            break;
                        }
                    }
                    
                    break;
            case "read_profiles":
                for(profile in data.value) {
                    spans = Array.from(document.querySelectorAll(`span[data-id*="${data.value[profile].id}"]`));
                    for(span in spans) {
                        spans[span].replaceChildren(document.createTextNode(data.value[profile].name));
                    }
                    imgs = Array.from(document.querySelectorAll(`img[data-id*="${data.value[profile].id}"]`));
                    for(img in imgs) {
                        imgs[img].setAttribute("src", data.value[profile].avatar);
                    }

                    known_profiles[data.value[profile].id] = data.value[profile];
                }

                break;
            case "read_user":
                document.getElementById("user_name").replaceChildren(document.createTextNode(data.value.name));
                document.getElementById("avatar").src = data.value.avatar;
                document.getElementById("number_of_messages").value = data.value.nr_messages;
        }
            
    };    

    function request(request, parameters) {
        socket.send(JSON.stringify({
            request: request,
            private_id: private_id,
            parameters: parameters
        }));
    }

    function create_channel() {
        request("create_channel", {
            channel_name: document.getElementById('channel_name').value,
        });
        document.getElementById('channel_name').value = "";
    }

    function create_user() {
        request("create_user", {
            user_name: document.getElementById('new_user').value,
            password: document.getElementById('password').value,
        });
    }

    function delete_channel(id) {
        if(confirm("Really delete?")) {
            request("delete_channel", {
                id: id
            });
        }
    }

    function delete_user(id) {
        if(confirm("Really delete?")) {
            request("delete_user", {
                id: id
            });
        }
    }

    function check_user() {
        if(document.cookie.startsWith("user_id=")) {
            ids = document.cookie.slice(8).split(":");
            private_id = ids[0];
            public_id = ids[1];
        } else {
            document.location.replace("login.html");
        }
    }

    function init_index() {
        console.log("init");
        check_user();
        if(socket.readyState == socket.OPEN) {
            request("read_channels");
            request("read_users");
        } else {
            socket_open_todos.push(init_index);
        }
    }

    function init_chat() {
        console.log("init");
        check_user();
        if(socket.readyState == socket.OPEN) {
            const searchParams = new URLSearchParams(window.location.search);
            if(searchParams.has('id')) {
                channel_id = searchParams.get('id');
                request("read_channel", {
                    id: channel_id,
                });
                channel_id = searchParams.get('id');
            } else {
                channel_id = null;
            }
        } else {
            socket_open_todos.push(init_chat);
        }
    }

    function init_user() {
        console.log("init");
        check_user();
        if(socket.readyState == socket.OPEN) {
            const searchParams = new URLSearchParams(window.location.search);
            if(searchParams.has('id')) {
                user_id = searchParams.get('id');
                request("read_user", {
                    id: user_id,
                });
            }
        } else {
            socket_open_todos.push(init_user);
        }

    }

    function add_message(parent, message) {
        if(parent == channel_id) {
            img_src = document.getElementById("image-root").src;
        } else {
            img_src = document.getElementById(`image-${parent}`).src;
        }
        if(img_src === "") {
            img_src = null;
        }

        request("message", {
            "message": message,
            "image": img_src,
            "channel": parent,
            "user": private_id,
        });

        if(parent == channel_id) {
            document.getElementById("message_text").value = "";
            document.getElementById("image-root").hidden = true;
        } else {
            document.getElementById("reply").remove();
        }
    }

    function login() {
        request("login", {
            "user_name": document.getElementById("user_name").value,
            "password": document.getElementById("password").value,
        });
    }

    function logout() {
        document.cookie = "";
        document.location.replace("login.html");
    }

    function get_channel_id() {
        return channel_id;
    }

    function set_avatar() {
        const formData = new FormData();
        formData.append("file",  document.getElementById("new_avatar").files[0]);
        formData.append("private_id", private_id);
        fetch("/set_avatar", {
            method: "POST",
            body: formData,
        });
    }

    function image(event, parent) {
        function upload(file) {
            const formData = new FormData();
            formData.append("file",  file);
            formData.append("private_id", private_id);
            fetch("/upload", {
                method: "POST",
                body: formData,
            }).then((response) => {
                if (response.ok) {
                    return response.json(); 
                } else {
                    throw new Error('Network response was not ok'); 
                }
            }).then((data) => {
                console.log(data);
                image = document.getElementById(`image-${parent}`);
                image.src = data;
                image.removeAttribute("hidden");
            });
        }

        event.preventDefault();

        if (event.dataTransfer.items) {
            [...event.dataTransfer.items].forEach((item, i) => {
                // If dropped items aren't files, reject them
                if (item.kind === "file") {
                  const file = item.getAsFile();
                  console.log(`… file[${i}].name = ${file.name}`);
                  upload(file);
                }
            });
        } else {
            // Use DataTransfer interface to access the file(s)
            [...event.dataTransfer.files].forEach((file, i) => {
              console.log(`… file[${i}].name = ${file.name}`);
              upload(file);
            });
          }
    }

    return {
        init_index: init_index,
        init_chat: init_chat,
        init_user: init_user,
        create_channel: create_channel,
        create_user: create_user,
        delete_channel: delete_channel,
        delete_user: delete_user,
        add_message: add_message,
        login: login,
        logout: logout,
        reply: reply,
        get_channel_id: get_channel_id,
        set_avatar: set_avatar,
        image: image,
    };
})();