var communicatie = (function(){
    let needed_profiles = new Set();
    let known_profiles = {};
    let public_id = private_id = null;
    let channel_id = null;
    let socket = new WebSocket("wss://www.appelo.nl:8181/ws");
    
    socket.onopen = function(_event) {
        console.log("socket open");
    };

    socket.onerror = function(event) {
        console.error('onerror!!!', event);
    }

    function add_channel(list, channel) {
        item = document.createElement("li");
        item.dataset.id = channel.id;
        link = document.createElement("a");
        link.setAttribute("href", `javascript:communicatie.set_channel_id("${channel.id}");communicatie.show_page("chat.html", {"read_channel":{"id": "${channel.id}"}})`);
        link.appendChild(document.createTextNode(channel.name));
        item.appendChild(link);
        span = document.createElement("span");
        if(channel.properties.public) {
            span.textContent = "🔓";
        } else {
            span.textContent = "🔒"; 
        }
        item.appendChild(span);
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
        
        request("subscribe", {
            channel: message.id,
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
                    private_id = data.value.private_id;
                    public_id = data.value.public_id;
                    show_page("channels.html", {"read_channels": null, "read_users": null});
                } else {
                    show_error("Login failed.");
                }
 
                break;
            case "read_channels":
                list = document.getElementById("channels");
                for (channel of data.value) {
                    add_channel(list, channel);
                }

                break;
            case "read_users":
                list = document.getElementById("users");
                for (user of data.value) {
                    item = document.createElement("li");
                    item.dataset.id = user.id;
                    link = document.createElement("a")
                    link.setAttribute("href", `javascript:communicatie.show_page('user.html',  {'read_user': {'id': '${user.id}'}})`);
                    link.appendChild(document.createTextNode(user.name));
                    item.appendChild(link);
                    if(user.id === public_id) {
                        span = document.createElement("span");
                        span.setAttribute("class", "trash");
                        span.setAttribute("onclick", `communicatie.delete_user('${user.id}')`);
                        span.innerHTML = "&#x1F5D1;";
                        item.appendChild(span);
                    }
                    list.appendChild(item);
                }

                break;
            case "read_channel":
                header = document.getElementById("title");
                header.textContent = data.value.channel_name;
                if(!data.value.properties.public && data.value.owner === public_id) {
                    document.getElementById("user_icon").hidden = false;
                }
                request("subscribe", {
                    channel: data.value.id,
                });
                break;
            case "subscribe":
                for(message of data.value) {
                    if(message.parent === channel_id) {
                        parent = document.getElementById("messages");
                    } else {
                        parent = document.querySelector(`[data-id*="${message.parent}"]`);
                    }
                    append_message(parent, message);
                }
                request_user_profiles();
                break;
            case "message":
                if(data.value.parent === channel_id) {
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
                document.getElementById("title").textContent = data.value.name;
                document.getElementById("avatar").src = data.value.avatar ? data.value.avatar : "img/unknown.png";
                document.getElementById("new_avatar").hidden = document.getElementById("submit_avatar").hidden = (data.value.id !== public_id);
                document.getElementById("number_of_messages").value = data.value.nr_messages;
                break;
            case "read_members":
                members = document.getElementById("members");
                members.innerHTML = "";
                none_members = document.getElementById("none-members");
                none_members.innerHTML = "";
                for(user of data.value.members) {
                    item = document.createElement("li");
                    item.setAttribute("ondragstart", "communicatie.drag_member(event)");
                    item.dataset.id = user.id;
                    item.setAttribute("draggable", "true");
                    img = document.createElement("img");
                    img.src = user.avatar ? user.avatar : "img/unknown.png";
                    item.appendChild(img);
                    span = document.createElement("span");
                    span.textContent = user.name;
                    item.appendChild(span);
                    members.appendChild(item);
                }
                for(user of data.value["none-members"]) {
                    item = document.createElement("li");
                    item.setAttribute("ondragstart", "communicatie.drag_member(event)");
                    item.dataset.id = user.id;
                    item.setAttribute("draggable", "true");
                    img = document.createElement("img");
                    img.src = user.avatar ? user.avatar : "img/unknown.png";
                    item.appendChild(img);
                    span = document.createElement("span");
                    span.textContent = user.name;
                    item.appendChild(span);
                    none_members.appendChild(item);
                }
                break;
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
            public: document.getElementById('public').value === "public",
        });
        document.getElementById('channel_name').value = "";
        document.getElementById('public').value = "private";
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

    function show_error(message) {
        error = document.getElementById("error");
        error.innerHTML = message;
        error.hidden = false;
        setTimeout(() => {
            error.hidden = true;
        }, 5000);
    }

    function show_page(url, actions) {
        document.getElementById("user_icon").hidden = true;

        fetch(url).then((response) => {
            if (response.ok) {
                return response.text(); 
            } else {
                show_error('Network response was not ok'); 
            }
        }).then((data) => {
            document.getElementById("page").innerHTML = data;
            if(actions) {
                Object.keys(actions).forEach(action => {
                    if(actions[action] === null) {
                        request(action);
                    } else {
                        request(action, actions[action]);
                    }
                });
            }
        });
    }

    function init() {
        console.log("init");
        show_page("login.html");
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

    function get_channel_id() {
        return channel_id;
    }

    function set_channel_id(id) {
        channel_id = id;
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
                if (item.kind === "file") {
                  const file = item.getAsFile();
                  console.log(`name = ${file.name}`);
                  upload(file);
                  return;
                }
            });
        } else {
            [...event.dataTransfer.files].forEach((file, i) => {
              console.log(`name = ${file.name}`);
              upload(file);
              return;
            });
          }
    }

    function manage_users() {
        document.getElementById("user_manager").hidden = false;
        request("read_members", channel_id);
    }

    function member(event, is_member) {
        event.preventDefault();
        console.log(event);
        const id = event.dataTransfer.getData("text/plain");
        const dragged_element = document.querySelector(`[data-id*="${id}"]`);
        if (!dragged_element) return;

        if(is_member) {
            document.getElementById("members").appendChild(dragged_element);
        } else {
            document.getElementById("none-members").appendChild(dragged_element);
        }
        request("set_member", {
            channel: channel_id,
            user: id,
            is_member: is_member,
        });
}

    function drag_member(event) {
        event.dataTransfer.setData("text/plain", event.currentTarget.dataset.id);
        event.dataTransfer.effectAllowed = "move";
    }

    function unsubscribe_all() {
        request("unsubscribe_all");
    }
    
    return {
        init: init,
        create_channel: create_channel,
        create_user: create_user,
        delete_channel: delete_channel,
        delete_user: delete_user,
        add_message: add_message,
        login: login,
        reply: reply,
        get_channel_id: get_channel_id,
        set_channel_id: set_channel_id,
        set_avatar: set_avatar,
        image: image,
        show_page: show_page,
        manage_users: manage_users,
        member: member,
        drag_member: drag_member,
        unsubscribe_all: unsubscribe_all,
    };
})();