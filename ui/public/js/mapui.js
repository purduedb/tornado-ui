
var clientState = undefined;



if (typeof (Storage) !== "undefined") {
    // Code for localStorage/sessionStorage.

    if (localStorage.testarr) {
        console.log("found test arr");
    }
    else {
        console.log("nothing found");
        var o = {"content": [{"val": 1}, {"val": 2}]};
        localStorage.testarr = $.toJSON(o);
    }

} else {
    // Sorry! No Web Storage support..
}

var uiState = {
    catalog:{
        queries:{
            liststate:[],
            // keys:[],
            // vals:[],
            list:{},
            poller:null
        },
        datasources:{
            poller:null
        }
    },
    knnList: [],
    rangeList: [],
    getRandomColor: function () {
        return randomColor({luminosity: 'dark'});
    },    
    getRandomColorLight: function () {
        return randomColor({luminosity: 'light'});
    },  
    resolveRangePredVar: function (v) {
        var i = 0;
        while (i < uiState.rangeList.length) {
            if (v == uiState.rangeList[i].name) {
                console.log(uiState.rangeList[i].name);
                return createMBR(uiState.rangeList[i].rect);
            }
            i++;
        }
        return null;
    },
    resolveKNNPredVar: function (v) {
        var i = 0;
        console.log(v);
        while (i < uiState.knnList.length) {
            if (v == uiState.knnList[i].name) {
                // console.log(uiState.knnList[i].marker.getPosition());
//        var p = uiState.knnList[i].marker.getPosition();
                var marker = uiState.knnList[i].marker;

                return createLatLng(marker);
            }
            i++;
        }
        return null;
    }
};

/**
 flags
 **/

var queryvis = true;
var load200 = false;
var load800 = false;
///////////////////

var queries = [];
var inputpoints = [];
var trips = [];
var lines = [];

var eventsources = [];

var nodes = {};
var edges = {};
var nodeSet = [];

/////////////////
var gmap = null;
var pservice = null;

var beforeColor = '#CF5656';
var afterColor = '#9BC150';

var polylines = [];

var drawingContext = {
    rectangles: [],
    rectangleMaxCount: 3
};
var drawingManager = null;

var beforeRectOptions = {
    clickable: true,
    draggable: true,
    editable: true,
    // fillColor: beforeColor,
    fillOpacity: 0,
    strokeWeight: 5
};

// place holder object for the request
var request = {
    mbr: null
};




var drawingOptions = {
    drawingMode: null, //google.maps.drawing.OverlayType.RECTANGLE,
    drawingControl: true,
    drawingControlOptions: {
        position: google.maps.ControlPosition.TOP_CENTER,
        drawingModes: [
            // google.maps.drawing.OverlayType.CIRCLE,
            // google.maps.drawing.OverlayType.POLYGON,
            google.maps.drawing.OverlayType.MARKER,
            google.maps.drawing.OverlayType.RECTANGLE
        ]
    },
    markerOptions: {draggable: true},
    rectangleOptions: beforeRectOptions

};


var es; // event stream handle

function syslogAddStatus(smsg){
    if (smsg.status === "success"){
        $("#syslog").append(createPanelElement("panel-success",smsg.msg));
    }
    else if (smsg.status === "error"){
        $("#syslog").append(createPanelElement("panel-danger",smsg.msg));
    }
}

function createPanelElement(panelClass, bodyContent){
    return $("<div class='panel "+panelClass+"'></div>").append("<div class='panel-body'>"+bodyContent+"</div>");
}


function initialize() {

    //state listeners
    // uiState.catalog.queryStateListener = setInterval(function(){
    //     refreshRegiesteredQueryList();
    // },500);

    refreshRegiesteredQueryList();

    $.ajax("tornado/config").done(function(conf){
        console.log(conf);
        if (conf.kafka.enabled){
            uiState.es = new EventSource("kafka/output-stream2");
            console.log("listening to server events : kafka/output-stream2")
        }
        else {
            uiState.es = new EventSource("/mock-output-stream");
        }

        uiState.es.onmessage = function(e) {
        //console.log("got an sse");
        var sse = $.parseJSON(e.data);
        var ssetype = sse.type;
        if (conf.kafka.enabled && (typeof sse.data) === "string"){
            // console.log(sse);
            // console.log(sse.data);    
            sse = eval("(" + sse.data + ")");
        }
        // console.log(e);
        // console.log(sse);
        // console.log($.toJSON(sse)); 
        
        if (ssetype == "output") {

            var color = uiState.getRandomColor();

            if (sse.outputColor) {
                color = sse.outputColor;
            }

            // lookup query current state
            var qo;

            if (sse.name === undefined){
                qo = uiState.catalog.queries.list[sse.qname];
            }
            else {
                qo = uiState.catalog.queries.list[sse.name];
            }

            if (qo !== undefined){

                if (qo.outputObjects === undefined){
                    qo.outputObjects = {};
                }

                if (sse.oid !== undefined){
                    // single tuple
                    //console.log(qo.outputObjects[sse.oid]);
                    if (sse.tag !== undefined){
                        if (sse.tag === "+"){
                            addOutputObject(sse, qo);
                        }
                        else if (sse.tag === "-"){
                            deleteOutputObject(qo,sse.oid);                        
                        }
                        else if (sse.tag === "u"){
                            if (qo.outputObjects[sse.oid] === undefined){
                                console.log("WARN : u for oid that does not exist in state");
                                addOutputObject(sse, qo); 
                            }
                            else {                                
                                deleteOutputObject(sse, qo);
                                addOutputObject(sse, qo); 
                            }
                        }
                    }
                    else {
                        //do nothing
                    }
                    
                }
                else if (sse.oid1 !== undefined && sse.oid2 !== undefined){
                    // joined tuple

                    if (sse.tag !== undefined){
                        if (sse.tag === "+"){
                            addOutputObjectPair(sse, qo);
                        }
                        else if (sse.tag === "-"){
                            deleteOutputObjectPair(sse, qo);                        
                        }
                        else if (sse.tag === "u"){
                            if (qo.outputObjects[sse.oid1] === undefined || qo.outputObjects[sse.oid2] === undefined){
                                console.log("WARN : u for oid1 and oidt that do not exist in state");
                                addOutputObjectPair(sse, qo);
                            }
                            else {                                
                                deleteOutputObject(sse, qo);
                                addOutputObject(sse, qo); 
                            }
                        }
                    }
                    else {
                        //do nothing
                    }
                }
                // store output elements
                //qo.output.push(outputEntry);


                refreshOutputListing();

            }
            else {
                console.log("WARN: query object is not defined : " + sse.name);
            }


        }
    }
    });
                       
    var mapOptions = {
        // US 
        // 39.730255, -98.018183
        //24.507052, 45.371521
        // center : 39.338734, -100.058701

        // berlin
        // 52.520190250694526&west=13.405380249023438&south=52.51914570999911&east=13.407440185546875 
        // makkah : 22.473878, 40.121263
        //center: new google.maps.LatLng(39.730255, -98.018183),
        //usCenter
        center: new google.maps.LatLng(39.338734, -100.058701),
        zoom: 4
    };
    var map = new google.maps.Map(document.getElementById("map-canvas"),
            mapOptions);
    gmap = map;
    //pservice = new google.maps.places.PlacesService(gmap);
    drawingOptions.rectangleOptions = beforeRectOptions;

    drawingManager = new google.maps.drawing.DrawingManager(drawingOptions);
    drawingManager.addListener("markercomplete", function (m) {
        if ((uiState.knnList.length) == drawingContext.rectangleMaxCount) { //TODO change limit
            m.setMap(null);
        }
        else {
            var qid = uiState.knnList.length;
            var _color = uiState.getRandomColorLight();
            var _iconUrl = createColoredMarker(_color);
            uiState.knnList.push({show: true, name: "f" + qid, marker: m, color: _color, iconUrl: _iconUrl});

            var nopt = {
                icon: _iconUrl
            };

            //console.log(m);
            m.setOptions(nopt);

            m.addListener("position_changed", function () {
                updateKNNPred(qid, this, {});
            });
            updateKNNList();
        }
    });
    drawingManager.addListener("rectanglecomplete", function (e) {
        // console.log("a rectangle is completed");
        // console.log(e.getBounds());
        // console.log("current count = "+drawingContext.rectangles.length);
        
        if ((uiState.rangeList.length) == drawingContext.rectangleMaxCount) {
            // console.log("max count has reached which is "+drawingContext.rectangleMaxCount);
            e.setMap(null);
        }
        else {
            var qid = uiState.rangeList.length;

            var _color = uiState.getRandomColorLight();

            var rnopt = {
               clickable: false,
               draggable: false,
               editable: false,
                fillColor: _color,
                fillOpacity: 0.12,
                strokeWeight: 1
            };

            e.setOptions(rnopt);

            uiState.rangeList.push({show: true, name: "r" + qid, rect: e, color: _color});

            e.addListener("bounds_changed", function () {
                updateMBR(qid, this);
            });
            updateRangeList();
        }

    });

    drawingManager.setMap(map);


    // code mirror
    // console.log($("#textEntry").get(0));
    // console.log(document.body);
    var mime = 'text/x-mariadb';
    // get mime type
    if (window.location.href.indexOf('mime=') > -1) {
    mime = window.location.href.substr(window.location.href.indexOf('mime=') + 5);
    }
    uiState.editor = CodeMirror.fromTextArea($("#textData").get(0), {
    mode: mime,
    indentWithTabs: true,
    smartIndent: true,
    lineNumbers: true,
    matchBrackets : true,
    autofocus: true,
    extraKeys: {"Ctrl-Space": "autocomplete"},
    hintOptions: {tables: {
      users: {name: null, score: null, birthDate: null},
      countries: {name: null, population: null, size: null}
    }}});


    populateDataSources();
}

// initialize data source list
function populateDataSources(){
    

    $.getJSON( "tornado/datasources", function( data ) {
      var target = $("#regDatasources .list-group");
      target.empty();

      $.each( data, function( key, val ) {
        //console.log(key);
        //console.log(val);
        target.append( "<li id='" + key + "' class='list-group-item' >" + val.sourceName + "</li>" );
      });     
    });
}


function buildErrorMessage(e) {
    return e.line !== undefined && e.column !== undefined
      ? "Line " + e.line + ", column " + e.column + ": " + e.message
      : e.message;
}

function addOutputObject(sse, qo){
    var outobj = qo.outputObjects[sse.oid];
    var iconUrl = createColoredMarker(qo.outputColor);
    var outputEntry = {};
    var outputlisting = $("#outputlisting");

    if ( outobj === undefined){
        // TODO: utilize tags
        // check if object is already displayed

        // create map element
        var myLatlng1 = new google.maps.LatLng(sse.point.lat,sse.point.lng);
        var marker1 = new google.maps.Marker({
              position: myLatlng1,
              icon: iconUrl,
              title:sse.text
          });
        var infowindow1 = new google.maps.InfoWindow({
              content: "<p>"+sse.text+"</p>"
          });
        google.maps.event.addListener(marker1,'click',function(){
            infowindow1.open(gmap,marker1);                
         });

        // create list element

        var listitem = createOutputTupleListEntry(sse,qo);

        if (qo.checked === "check"){
            marker1.setMap(gmap);                    
        }
        else{
            listitem.toggleClass("hidden");
        }
        
        outputlisting.append( listitem );

        // if (!qo.output){
        //     qo.output = [];
        // }
        
        outputEntry.mapElement = marker1;
        outputEntry.listElement = listitem;   
        outputEntry.queryObject = qo;

        qo.outputObjects[sse.oid] = outputEntry; 


        refreshOutputListing();
    }
    else{
        console.log("warning : + for oid that already exist in state");
    }

}

function addOutputObjectPair(sse, qo){

    // create either one that does not exist otherwise create both. No listing required

    var outobj1 = qo.outputObjects[sse.oid1];
    var outobj2 = qo.outputObjects[sse.oid2];

    var iconUrl = createColoredMarker(qo.outputColor);
    var outputlisting = $("#outputlisting");

    if ( outobj1 === undefined && outobj2 === undefined){
        // both objects are new
        // console.log(sse);

        var listitem = createJoinOutputTupleListEntry(sse,qo);

        var toe = joinTwoOutputEntries(sse, createOutputEntry(qo,sse.oid1,sse.point1.lat,sse.point1.lng,sse.text1),
                                            createOutputEntry(qo,sse.oid2,sse.point2.lat,sse.point2.lng,sse.text2),
                                            listitem);

        // display elements
        displayJoinedTuple(sse, qo,toe, listitem);

    }
    else if (outobj1 !== undefined && outobj2 === undefined){
        
        var listitem = createJoinOutputTupleListEntry(sse,qo);

        var toe = joinTwoOutputEntries(sse, outobj1,
                                    createOutputEntry(qo,sse.oid2,sse.point2.lat,sse.point2.lng,sse.text2),
                                    listitem);

        displayJoinedTuple(sse, qo,toe, listitem);        

    }
    else if (outobj1 === undefined && outobj2 !== undefined){

        var listitem = createJoinOutputTupleListEntry(sse,qo);

        // var toe = joinTwoOutputEntries(sse, createOutputEntry(sse,qo), outobj2,listitem);
        var toe = joinTwoOutputEntries(sse, createOutputEntry(qo,sse.oid1,sse.point1.lat,sse.point1.lng,sse.text1),
                                        outobj1,                
                                        listitem);
        displayJoinedTuple(sse, qo,toe, listitem);       
    }
    else{
        console.log("warning : adding request for objects that already exist in state");
    }

}

function displayJoinedTuple(sse, qo, arr, listitem){

    var outputEntry1 = arr[0];
    var outputEntry2 = arr[1];

    // display markers on map
    if (qo.checked === "check"){       
        outputEntry1.mapElement.setMap(gmap);
        outputEntry2.mapElement.setMap(gmap);
        outputEntry1.joinLine[sse.oid2].setMap(gmap);
    }
    else {
        listitem.toggleClass("hidden");
    }

    // append list entry
    $('#outputlisting').append(listitem);

}

function deleteOutputObject(sse,qo) {

    var outobj = qo.outputObjects[sse.oid];
    if (outobj === undefined){
        console.log("warning : deleting oid(s) that DO NOT exist in state");
    }
    else {
        if (qo.checked === "check"){
            // object is displayed, remove from display first
            outobj.mapElement.setMap(null);
            outobj.listElement.remove();
        }

        delete qo.outputObjects[oid];
    }
}

function deleteOutputObjectPair(sse,qo) {

    var outobj1 = qo.outputObjects[sse.oid1];
    var outobj2 = qo.outputObjects[sse.oid2];

    if (outobj1 === undefined && outobj2 === undefined){
        // nothing to do
        console.log("warning : deleting oid(s) that DO NOT exist in state");
    }
    else if (outobj1 !== undefined && outobj2 === undefined){
        if (qo.checked === "check"){
            // object is displayed, remove from display first
            outobj1.mapElement.setMap(null);
            outobj1.listPair[sse.oid2].remove();
            outobj1.joinLine[sse.oid2].setMap(null);
            delete outobj1.joinLine[sse.oid2];
            delete outobj1.listPair[sse.oid2];
        }

        delete qo.outputObjects[oid1];

    }
    else if (outobj1 === undefined && outobj2 !== undefined){
        if (qo.checked === "check"){
            // object is displayed, remove from display first
            outobj2.mapElement.setMap(null);
            outobj2.listPair[sse.oid1].remove();
            outobj2.joinLine[sse.oid1].setMap(null);
            delete outobj2.joinLine[sse.oid1];
            delete outobj2.listPair[sse.oid1];

        }

        delete qo.outputObjects[oid2];
    }
    else {
        if (qo.checked === "check"){
            // object is displayed, remove from display first
            outobj1.mapElement.setMap(null);
            outobj1.listPair[sse.oid2].remove();
            outobj2.mapElement.setMap(null);
            outobj2.listPair[sse.oid1].remove();
            outobj1.joinLine[sse.oid2].setMap(null);
            outobj2.joinLine[sse.oid1].setMap(null);

            delete outobj1.joinLine[sse.oid2];
            delete outobj2.joinLine[sse.oid1];

            delete outobj2.listPair[sse.oid1];
            delete outobj1.listPair[sse.oid2];
        }

        delete qo.outputObjects[oid1];
        delete qo.outputObjects[oid2];
    }
}

function refreshOutputListing(){
    var outputlisting = $("#outputlisting");
    var t = outputlisting.find("li.empty:not(.hidden)");
    // console.log(t);
    if (t.length  > 0) {
        // list is empty but contains an info bullet, so make it empty
        // outputlisting.empty();  
        
        t.addClass("hidden");
        
    }
    if (outputlisting.find("li.list-group-item.hidden:not(.empty)").length == outputlisting.find("li.list-group-item:not(.empty)").length){
        // show empty 
        outputlisting.find("li.empty.hidden").removeClass("hidden");
    }
}


function createPredicateListEntry(pobj) {
    var cqobj = pobj;
    
    var colorIcon = $('<span class="glyphicon glyphicon-stop" style="float:left;margin-right:0.6em;color:'+cqobj.color+'"></span>');
    var removeIcon = $('<span class="glyphicon glyphicon-trash" style="float:left;margin-right:0.6em;color:black"></span>');
    var pname = $('<p>'+pobj.name+'</p>');
    var res = $('<li class="list-group-item"></li>').append(colorIcon).append(removeIcon).append(pname);
    removeIcon.click(function(){
        // TODO: remove predicate
    });
    return res;
}

function createOutputEntry(qo,oid,lat,lng,textv){
    var iconUrl = createColoredMarker(qo.outputColor);
    var myLatlng1 = new google.maps.LatLng(lat,lng);
    var marker1 = new google.maps.Marker({
          position: myLatlng1,
          icon: iconUrl,
          title:textv
      });
    var infowindow1 = new google.maps.InfoWindow({
          content: "<p>"+textv+"</p>"
      });
    google.maps.event.addListener(marker1,'click',function(){
        infowindow1.open(gmap,marker1);                
     });

     var outputEntry1 = {};
    // outputEntry1.joinLine = {};
    outputEntry1.listPair = {};
    outputEntry1.joinLine = {};
    outputEntry1.mapElement = marker1;
    //outputEntry1.listPair[sse.oid2] = listitem; 
    // console.log(qo);
    outputEntry1.queryObject = qo;
    qo.outputObjects[oid] = outputEntry1;
    return outputEntry1;
}


function joinTwoOutputEntries (sse, ot1,ot2, listitem) {
    
    // create join line
    var joinLine_coor = [
        ot1.mapElement.getPosition(),
        ot2.mapElement.getPosition()
    ];

    // console.log(joinLine_coor);
    
    var joinLine = new google.maps.Polyline({
        path: joinLine_coor,
        geodesic: true,
        strokeColor: '#000000',
        strokeOpacity: 1.0,
        strokeWeight: 2
      });

    ot1.joinLine[sse.oid2] = joinLine;
    ot2.joinLine[sse.oid1] = joinLine;

    ot1.listPair[sse.oid2] = listitem;
    ot2.listPair[sse.oid1] = listitem;

    return [ot1,ot2];
}

function createOutputTupleListEntry(sse,qo){
    var iconUrl = createColoredMarker(qo.outputColor);
    var iconElem = $("<img src='"+iconUrl+"' />");
    iconElem.click(function(){
        gmap.panTo(new google.maps.LatLng(sse.point.lat,sse.point.lng));
        gmap.setZoom(10);
    });
    
    var scolor = "white";
    var bcolor = "white";
    var fcolor = "#000000";
    var eclass = "";
    var brcolor = "#ddd";

    var vnegre = /\[Very negative\]/
    var negre = /\[Negative\]/
    var vposre = /\[Very positive\]/
    var posre = /\[Positive\]/
    var neutre = /\[Neutral\]/
    
    if(vposre.test(sse.text)){
        // console.log("Very Positive");
        // scolor = "rgba(0,255,0,1)";        
        bcolor = "#7DCF68";
        fcolor = "#1B5F09";
        brcolor = "#1B5F09"; // border color

    }

    if(posre.test(sse.text)){
        // console.log("Positive");
        // scolor = "rgba(0,255,0,0.5)";
        bcolor = "#A4E394"
        fcolor = "#1B5F09";
        brcolor = "#1B5F09"; // border color
    }

    if(negre.test(sse.text)){
        // console.log("Negative");
        // scolor = "rgba(255,0,0,0.5)";
        bcolor = "#FDD5D7";
        fcolor = "#B8494F";
        brcolor = "#B8494F"; // border color
    }

    

    if(vnegre.test(sse.text)){
        // console.log("Very Negative");
        // scolor = "rgba(255,0,0,1)";
        bcolor = "#FBB4B8";
        fcolor = "#B8494F";
        brcolor = "#B8494F"; // border color
    }

    

    if(neutre.test(sse.text)){
        // console.log("Neutral");
        bcolor = "#A7A7A7";
        // fcolor = "#7D151A";
        // brcolor = "#ddd"; // border color
    }

    // var iconCol = $("<span class='col-md-2' style='background-color:"+scolor+";'></span>").append(iconElem);
    var iconCol = $("<span class='col-md-2' class='"+eclass+"'></span>").append(iconElem);
    // var textCol = $("<p class='col-md-8' style='word-wrap:break-word;overflow-x:scroll;'>"+sse.text+"</p>");
    var textCol = $("<p class='col-md-8' style='word-wrap:break-word;'>"+sse.text+"</p>");
    var tupleentry = $("<div class='row'></div>").append(iconCol).append(textCol);
    var listitem = $("<li class='list-group-item' style='background-color:"+bcolor+";color:"+fcolor+";border-color:"+brcolor+";' ></li>").append(tupleentry);

    return listitem;
}

function createJoinOutputTupleListEntry(sse,qo){
    var iconUrl = createColoredMarker(qo.outputColor);
    var iconElem1 = $("<img src='"+iconUrl+"' />");
    iconElem1.click(function(){
        gmap.panTo(new google.maps.LatLng(sse.point1.lat,sse.point1.lng));
        gmap.setZoom(10);
    });
    var iconElem2 = $("<img src='"+iconUrl+"' />");
    iconElem2.click(function(){
        gmap.panTo(new google.maps.LatLng(sse.point2.lat,sse.point2.lng));
        gmap.setZoom(10);
    });
    var iconElemJoin = $("<svg style='margin-left:3px;' height='14' width='14'><path d='M 0,0 L 0,0 14,14 L 14,14 14,0 L 14,0 0,14 L 0,14 0,0  Z' /></svg>");
    var iconCol = $("<span class='col-md-2'></span>").append(iconElem1).append(iconElemJoin).append(iconElem2);
    var textCol1 = $("<p class='col-md-8' style='word-wrap:break-word; overflow-x:scroll;'>"+sse.text1+"</p>");
    var textCol2 = $("<p class='col-md-8' style='word-wrap:break-word;overflow-x:scroll;'>"+sse.text2+"</p>");
    var tupleentry = $("<div class='row'></div>").append(iconCol).append(textCol1).append(textCol2);
    var listitem = $("<li class='list-group-item' ></li>").append(tupleentry);

    return listitem;
}


function createQueryListEntry(qobj){
    var cqobj = qobj;
    var djo = {};
    djo.name = cqobj.name;
    
    // add tag for deletion
    djo.tag = "-";
    var colorIcon = $('<span class="glyphicon glyphicon-stop" style="float:left;margin-right:0.6em;color:'+cqobj.outputColor+'"></span>');
    var removeIcon = $('<span class="glyphicon glyphicon-trash" style="float:left;margin-right:0.6em;color:black"></span>');
    removeIcon.click(function(){
        $.ajax("tornado/queries",{
            method: "DELETE",
            data: $.toJSON(djo)
        }).done(function(resp){
            // console.log(resp);
            syslogAddStatus(resp);

            var dqo = uiState.catalog.queries.list[djo.name];

            for (i in dqo.outputObjects){
                var oe = dqo.outputObjects[i];
                // console.log(oe);

                oe.mapElement.setMap(null);
                if (oe.listElement !== undefined){
                    oe.listElement.remove();    
                }
                
                for (k in oe.listPair){
                    // console.log("deleting "+$.toJSON(oe.listPair[k]));
                    oe.listPair[k].remove();
                }

                if (oe.joinLine !== undefined){
                    var lines = oe.joinLine;
                    for (j in lines) {
                        lines[j].setMap(null);
                    }
                }

            }

            delete uiState.catalog.queries.list[djo.name];
            refreshRegiesteredQueryList();
            refreshOutputListing();
        });
    });
    var qname = $('<div type="button" data-toggle="collapse" data-target="#sqltext-'+djo.name+'" aria-expanded="false" aria-controls="sqltext='+djo.name+'">'
        +djo.name+'</div>');

    var sqltextwell = $('<div class="collapse" id="sqltext-'+djo.name+'"><div class="well">'+cqobj.sqltext+'</div></div>');

    var displayIcon = $('<span class="glyphicon glyphicon-'+cqobj.checked+'" style="float:left;margin-right:0.6em;color:black"></span>');
    displayIcon.click(function(){
        displayIcon.toggleClass("glyphicon-check glyphicon-unchecked"); 
        //console.log(displayIcon.is(".glyphicon-check"));
        if (displayIcon.is(".glyphicon-unchecked")){
            // update state
            cqobj.checked = "unchecked";
            // console.log("remove all output for "+djo.name+" from display");

            for (i in cqobj.outputObjects) {
                // hide marker from display
              cqobj.outputObjects[i].mapElement.setMap(null);

              // hide lines from display if applicable
              if (cqobj.outputObjects[i].joinLine !== undefined) {
                var lines = cqobj.outputObjects[i].joinLine;
                for (j in lines) {
                    lines[j].setMap(null);
                }
              }

              // hide list entries from display if applicable
              $(cqobj.outputObjects[i].listElement).toggleClass("hidden");

              if (cqobj.outputObjects[i].listPair !== undefined) {
                var listelemPairs = cqobj.outputObjects[i].listPair;

                for (j in listelemPairs){
                    if (!listelemPairs[j].is(".hidden")){
                        listelemPairs[j].addClass("hidden");        
                    }   
                }
              }
            };         

        }
        else if (displayIcon.is(".glyphicon-check")){
            cqobj.checked = "check";
            for (i in cqobj.outputObjects) {
              cqobj.outputObjects[i].mapElement.setMap(gmap);   
              if (cqobj.outputObjects[i].joinLine !== undefined) {
                var lines = cqobj.outputObjects[i].joinLine;
                for (j in lines) {
                    lines[j].setMap(gmap);
                }
              }          
              $(cqobj.outputObjects[i].listElement).toggleClass("hidden");

              //console.log("list element : "+cqobj.outputObjects[i].listElement.html);
              if (cqobj.outputObjects[i].listPair !== undefined) {
                var listelemPairs = cqobj.outputObjects[i].listPair;
                
                for (j in listelemPairs){
                    //console.log("hiding on check "+$listelemPairs[j].html());
                    if (listelemPairs[j].is(".hidden")){
                        listelemPairs[j].removeClass("hidden");    
                    }
                    
                }
              }
            };              
        }

    });

    return $('<li class="list-group-item qdata"></li>').append(colorIcon).append(removeIcon).append(displayIcon).append(qname).append(sqltextwell);
}

function refreshRegiesteredQueryList(){
    $.ajax("tornado/queries").done(function(data){
            // console.log($("#regQueriesList li.empty").length);
            // console.log($("#regQueriesList li").length);
            //syslogAddStatus(data);
            // console.log(data);
            // console.log(uiState.catalog.queries.liststate);
            var qlist = $("#regQueriesList")
            var emptyState = '<li class="list-group-item empty">No registered continuous queries</li>';
            if (data.length > 0) {

                // cache data
                // check for new queries registered

                for (var i = data.length - 1; i >= 0; i--) {
                    if (uiState.catalog.queries.list[data[i].name] === undefined){
                        // new query 
                        var nqo = data[i];
                        nqo.checked = "check";
                        nqo.output = [];
                        uiState.catalog.queries.list[nqo.name] = nqo; 
                    }

                    // var qi = uiState.catalog.queries.keys.indexOf(data[i].name);
                    // if (qi === -1){
                    //    // new query update client state
                    //    qi = uiState.catalog.queries.keys.push(data[i].name) -1;    
                    //    var nqo = data[i];
                    //    nqo.checked = "check";
                    //    nqo.output = [];
                    //    //console.log(nqo);
                    //    uiState.catalog.queries.vals.push(nqo); 
                    //    uiState.catalog.queries.list[nqo.name] = nqo;
                    //    //console.log(uiState.catalog.queries.list[nqo.name]);
                    // }                    
                };
                    //uiState.catalog.queries.liststate = data;
                    //console.log("here?");            

                    // check if qlist is currently empty (has the empty message)
                    if (qlist.find("li.empty").length > 0) qlist.empty();    

                    if (qlist.find("li").length != data.length) {
                        qlist.empty();

                        // for (var i = 0; i < uiState.catalog.queries.vals.length; i++) {
                             // console.log(data[i]);
                             // console.log(data[i].outputColor);
                            // qlist.append(createQueryListEntry(i));
                        // };

                        for (q in uiState.catalog.queries.list) {
                            // console.log(q + " : "+ $.toJSON(uiState.catalog.queries.list[q]));
                            qlist.append(createQueryListEntry(uiState.catalog.queries.list[q]));

                        }
                    }            
            }
            else{
                if (qlist.find("li.empty").length == 0) {
                    qlist.empty();    
                    qlist.append($(emptyState));
                }
                
            }

        });
}


function updateRegisteredQueries(qast, msg){
    syslogAddStatus(msg);
    // console.log(qast);
    var qlist = $("#regQueriesList");
    // check if qlist is currently empty (has the empty message)
    // if (qlist.find("li.empty").length > 0) qlist.empty();    
    // qlist.append(createQueryListEntry(qast));
    // The following will require a trip to the server to fetch the query list and update local state, can we update locally only ??
    refreshRegiesteredQueryList();            
}


// Doc load 
$(document).ready(function () {



    nodes = new vis.DataSet();
    edges = new vis.DataSet();

    // create a network
    var container = document.getElementById('visholder');
    var data = {
        nodes: nodes,
        edges: edges
    };
    var options = {
        hierarchicalLayout: true,
        shape: "database",
        smoothCurves: false,
        edges: {
            style: 'arrow'
        },
        physics:{
            "hierarchicalRepulsion": {
              "centralGravity": 0,
              "springLength": 0,
              "springConstant": 0,
              "nodeDistance": 10,
              "damping": 0
            },
            "maxVelocity": 57,
            "solver": "hierarchicalRepulsion",
            "timestep": 0.01
        }
    };
    var network = new vis.Network(container, data, options);
    
    /** Parser Code **/
    var grammartxt = $("#SyncSQLGrammar").text().trim();
    
    $.ajax("grammar.txt").done(function(data){
        //console.log(data);
        uiState.grammartxt = data;
        uiState.parser = PEG.buildParser(data, uiState);
    })

    //console.log(grammartxt);
    //var parser = PEG.buildParser(grammartxt, uiState);


    $("#buttonPanel span").click(function (event) {
        $("#textData").val($(this).attr('data'));
    });

    $('#submitSQL').click(function () {
        var ast = syncSQLparse(uiState.parser);
        if (ast !== null){
            var gmapView = createMBR(gmap);
            // console.log($.toJSON(gmapView));

            // add current view
            ast.currentView = gmapView;

            // add color 
            ast.outputColor = uiState.getRandomColor();
            //console.log(ast.outputColor);

            //optimizeQueryPlan(ast.plan);

            // post query submitted query to server
            $.ajax( "tornado/queries", {
                method: "POST",
                data: $.toJSON(ast),
                dataType: "json"
            }).done(function(data){
                //console.log(data);
                // syslogAddStatus(data);
                updateRegisteredQueries(ast,data);

            });

        }
        else {
            // parse error

        }
        
    });

    // Intialize google maps
//                google.maps.event.addDomListener(window, 'load', initialize);
    initialize();

    // set examples menu
    $('#rqex1').click(function(){
        // console.log(uiState.editor.getDoc().getValue());
        uiState.editor.getDoc().setValue($('#rq1r0').text());           
    });

    $('#knnqex1').click(function(){
        uiState.editor.getDoc().setValue($('#knnqf0').text());    
    });

    $('#knnqex2').click(function(){
        uiState.editor.getDoc().setValue($('#knnjq').text());    
    });

    $('#jqex').click(function(){
        uiState.editor.getDoc().setValue($('#rjq2r0').text());    
    });


    
    

    getDemoData();
    
    $('#test').click(function (event) {
        var url = '';
    });
    
    $('#addNode').click(function (event) {
        addNode();
    });

    $('#addEdge').click(function (event) {
        addEdge();
    });


    function addNode() {

        var i = Math.floor((Math.random() * 100) + 1);
        nodes.add([{id: "n" + i, label: 'Node ' + i}]);
        console.log("addning a node id=" + i);
        console.log("length = " + nodes.length);
    }

    function addEdge() {
        var ids = nodes.getIds()
        var i = Math.floor((Math.random() * ids.length));
        var j = Math.floor((Math.random() * ids.length));
        if (i == j) {
            if (j == ids.length - 1) {
                j == 0;
            }
            else {
                j = j + 1;
            }
        }

        edges.add({from: ids[i], to: ids[j]})
        console.log("adding an edge between (" + ids[i] + "," + ids[j] + ")");
    }

    function syncSQLparse(_parser) {
        $("#pmsg").removeClass("alert-success alert-info alert-warning alert-danger");
        try {
            
            // var sqltext = $("#textData").val().trim();
            var sqltext = uiState.editor.getValue().trim();
            var pres = _parser.parse(sqltext);
            pres.sqltext = sqltext;
            // console.log(pres);
            // prettyPrintAST('prettyPrint',pres);
            
            //console.log($.toJSON(pres));
            // var plan = pres.plan;
//                        plan = optimizeQueryPlan(plan);
            // visualizeQueryPlan(plan);
            $("#pmsg").addClass("alert-success");
            $("#pmsg").text("Query submitted successfully.");
            return pres;
        } catch (err) {
            //$('#prettyPrint').text(jsDump.parse(err));
            var errmsg = buildErrorMessage(err);
            var msge = $("<div class='panel panel-primary'>"+errmsg+"</div>");
            $("#syslog").append(msge);
            //console.log($.toJSON(err));
            
            $("#pmsg").addClass("alert-danger");
            $("#pmsg").text(errmsg);
            return null;
        }
    }

    function getLevels(node) {

        if (node.children != undefined) {
            return getLevels(node.children[0]) + 1;
        }
        else {
            return 1;
        }
    }

    function addNodeQPlan(node, parentId, k, lv) {
        if (node != undefined) {
            var nodeId = node.type + "" + (++k);

            // check if it exists
            if (nodes.get(nodeId) != null) {
                nodeId += ""+(lv);
            }

            var nodeLabel = node.type + " : ";
            if (node.attributes != undefined) {
                nodeLabel += $.toJSON(node.attributes);
            }

            if (node.conditions != undefined) {

                // var conds = $.toJSON(node.conditions);
                var conds = node.conditions;
                //console.log(conds);
                //should only have one??
                for (var i = 0; i < node.conditions.length; i++) {
                    var cond = node.conditions[i];

                    nodeLabel += "\n";

                    if (i != 0) nodeLabel += " and ";

                    if ($.isPlainObject(cond.lhs))
                        nodeLabel += cond.lhs.sourceName + "." + cond.lhs.attributeName;
                    else nodeLabel += cond.lhs;

                    nodeLabel += "  " + cond.op; 

                    if (cond.argc == 3) {
                        nodeLabel += "(" + cond.cval + ") "; 
                    }
                    else {
                        nodeLabel += " ";
                    }

                    // nodeLabel += " , " 
                    if ($.isPlainObject(cond.rhs))
                        nodeLabel += cond.rhs.sourceName + "." + cond.rhs.attributeName;
                    else nodeLabel += "(" + cond.rhs + ") "; 
                };

                
            }

            if (node.name != undefined) {
                nodeLabel += node.name;
            }

            var n = [{id: nodeId, label: nodeLabel,shape:'text', level: (k)}];
            // console.log($.toJSON(n));
            nodes.add(n);
            // console.log("adding a node (" + nodeId + ")");
            edges.add({from: nodeId, to: parentId});
            // console.log("adding an edge between (" + nodeId + "," + parentId + ")");
            if (node.children != undefined) {
                // console.log("node children length of (" + nodeId + ")  = "+node.children.length);
                for (var i = 0; i < node.children.length; i++) {
                    addNodeQPlan(node.children[i], nodeId, k, lv);
                    // console.log("added child #"+i+"  for (" + nodeId + ")");
                }
                // console.log("finished adding all child nodes  for (" + nodeId + ")");
                
            }

        }


    }
    
    function visualizeQueryPlan(plan){
        // console.log("vizualizing : "+$.toJSON(plan));
        var i = 0;
        var rootId = plan.type + "" + (++i);
        // nodes.add([{id: rootId, label: plan.type+" : "+plan.attributes.toString()}]);
        var lv = getLevels(plan);
        // console.log("levels = " + lv);

        nodes.clear();
        edges.clear();

        // console.log("cleared nodes and edges");

        addNodeQPlan(plan, rootId, i, lv);

        // console.log("added nodes and edges");

    }

});

var markers = [];

function clearMap() {
    while (drawingContext.rectangles.length > 0) {
        drawingContext.rectangles.pop().setMap(null);
    }
    drawingOptions.rectangleOptions = beforeRectOptions;
    drawingManager.setOptions(drawingOptions);
    drawingManager.setMap(gmap);

    // remove all polylines

    while (polylines.length > 0) {
        polylines.pop().setMap(null);
    }


}

// place holder object for the request
var request = {
    mbr: null
};

// This function creates the MBR (spatial predicate) for the query
function updateMBR(qid, rect) {
    // console.log(qid);
    // console.log(uiState.rangeList[qid]);
    console.log(createMBR(rect));
    uiState.rangeList[qid].rect = rect;
}

function addRangePredEntry(contId, eid, qname) {
    $("#" + contId).append($('<li style="border:solid thin;border-color:black;"></li>').text(qname));
}

function updateRangeList() {
    $("#rangeList").empty();
    for (var i = 0; i < uiState.rangeList.length; i++) {
        var t = uiState.rangeList[i];
        //console.log(t);
        var pe = createPredicateListEntry(t);
        $("#rangeList").append(pe);
        // addRangePredEntry("rangeList", t.name, t.name);

    }
    ;
}


function updateKNNPred(qid, marker,opt) {
    // console.log(marker);
    marker.setOptions(opt);
    uiState.knnList[qid].marker = marker;
}

function addKNNPredEntry(contId, p) {
    $("#" + contId).append($('<li style="border:solid thin;border-color:' + p.color + ';"></li>').text(p.name));
}
function updateKNNList() {
    $("#knnList").empty();
    for (var i = 0; i < uiState.knnList.length; i++) {
        var p = uiState.knnList[i];
        // console.log(p);
        var pe = createPredicateListEntry(p);
        $("#knnList").append(pe);
        // addKNNPredEntry("knnList", p);
    }
    ;
}


function createLatLng(marker) {
    return {lat: marker.getPosition().lat(), lng: marker.getPosition().lng()}
}

function createMBR(rect) {
    return {
        north: rect.getBounds().getNorthEast().lat(),
        east: rect.getBounds().getNorthEast().lng(),
        south: rect.getBounds().getSouthWest().lat(),
        west: rect.getBounds().getSouthWest().lng()
    };
}

function createColoredMarker(pinColor) {
    // var pinColor = "FE7569";
    // handle pounsign
    //var pinColor = uiState.getRandomColor();

    if (pinColor[0] == '#'){
        pinColor = pinColor.slice(1).toUpperCase();
        //console.log(pinColor);
    }


    var pinImage = new google.maps.MarkerImage("http://chart.apis.google.com/chart?chst=d_map_pin_letter&chld=%E2%80%A2|" + pinColor,
            new google.maps.Size(21, 34),
            new google.maps.Point(0, 0),
            new google.maps.Point(10, 34));
    return pinImage.url;
}


function prettyPrintAST(target,ast){
    $('#'+target).text(jsDump.parse(ast));
}


function getDemoData(){
    var skydeck = $.parseJSON('{"lng":-87.6358852,"tags":[{"k":"name","v":"SkyDeck Chicago"},{"k":"operator","v":"Willis Tower"},{"k":"tourism","v":"attraction"}],"ts":"2015-03-19T19:48:41Z","nid":"2311635030","lat":41.8786383}');
    console.log(skydeck.lng);
    console.log(skydeck.lat);
    //gmap.setCenter({lat: 41.8786383, lng: -87.6358852});
    //gmap.setZoom(14);
}

function optimizeQueryPlan(plan){
    console.log("optimizing : "+$.toJSON(plan));
    var oper = plan;
    var parent = null;
    while (oper.type !== "stream"){
        //add back pointer to parent
//                    for (var i = 0; i < oper.children.length; i++){
//                        oper.children[i].parent = oper;
//                    }
        
//                    oper.children[0].parent = oper;
        console.log("parent added : "+$.toJSON(oper));
        
        if (oper.type === "select"){
            var leftchild = oper.children[0];
            if (leftchild.type === "stream" && leftchild.name === "places"){
                console.log("leftchild : "+$.toJSON(leftchild)+" , typeOf leftchild "+typeof(leftchild));
                //leftchild.parent = oper.parent;
                leftchild.keyword = oper.conditions[0][2];
//                            oper.parent.children[0] = leftchild; // assume left child for now
                parent.children = oper.children;
                oper = leftchild;
            }
            else{
                parent = oper;
                oper = oper.children[0];
            }
        }
        else{
            parent = oper;
            oper = oper.children[0];
        }
        
    }
    console.log("source : "+$.toJSON(oper));
    console.log("optimized : "+$.toJSON(plan));
    return plan;
}

function executeQueryOperator(oper){
    var cnum = oper.children.length;
    if (cnum === 1){
        // unary operator
        if (oper.type === "project"){
            var resset = executeOperator(oper.children[0]);
            for (i in resset) console.log(i)
        }
        
        if (oper.type === "select"){
            
        }
        
        if (oper.type === "stream"){
            if (oper.name === "places"){
                // use google API
            }
            else{
                console.log("Cannot resolve source:"+oper.name);
            }
        }
        
    }
    else if (cnum === 2){
        // binary
    }
}




