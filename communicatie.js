var communicatie = function(){

    let socket = new WebSocket("wss://www.appelo.nl:8181");
    socket.onopen = function(e) {
        socket.send('{"request": "channels"}');
    };
    socket.onerror = e =>{console.error('onerror!!!', e)}
    socket.onmessage = function(event) {
        console.log(`[message] Data received from server: ${event.data}`);
        data = JSON.parse(event.data);
        if(data.channels) {
            list = document.getElementById("1-vs-1");
            for (const channel in data.channels) {
                item = document.createElement("li");
                item.appendChild(document.createTextNode(data.channels[channel]));
                list.appendChild(item);
            }
        }
    };    

    function getChannels() {

    }

    return {
        getChannels: getChannels
    };
}();