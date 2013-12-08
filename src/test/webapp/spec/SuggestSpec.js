var URL = "http://localhost:8999/geocoder";
// var URL = "http://graphhopper.com/geocoder/api";

isArray = function(someVar) {
    return Object.prototype.toString.call(someVar) === '[object Array]';
};

isObject = function(someVar) {
    return someVar != null && typeof someVar === 'object';
};

assertObject = function(expectedObj, resultingObj) {
    for (var obj in expectedObj) {
        if (expectedObj.hasOwnProperty(obj) && resultingObj.hasOwnProperty(obj)) {
            expect(expectedObj[obj]).toBe(resultingObj[obj]);
            continue;
        }

        expect("-").toBe("result has no property " + obj);
    }
    return true;
};

expectSuggest = function(query, expectedResult) {
    var size = 6;
    $.ajax({
        "timeout": 1000,
        "url": URL + "?size=" + size + "&suggest=true&q=" + encodeURIComponent(query),
        "async": false,
        "success": function(json) {
            expect(json.hits.length).toBe(size);

            if (isArray(expectedResult)) {
                for (var i = 0; i < expectedResult.length; i++) {
                    var e = expectedResult[i];
                    if (isObject(e))
                        assertObject(e, json.hits[i]);
                    else
                        expect(e).toBe(json.hits[i].name);
                }
            } else if (isObject(expectedResult)) {
                assertObject(expectedResult, json.hits[0]);
            } else {
                var result = json.hits[0].name;
                expect(expectedResult).toBe(result);
            }
        },
        "error": function(err) {
            expect("-").toBe(err);
        },
        "type": "GET",
        "dataType": "json"
    });
};

describe("suggestion", function() {

    it("should find simple things", function() {
        expectSuggest("dresden", "Dresden");
        expectSuggest("dresd", "Dresden");
        // TODO expectSuggest("birkenstra", "Birkenstraße");

        expectSuggest("birkenh", [{name: "Birkenhof", type: "hamlet"}, {name: "Birkenhain", type: "village"}]);
        // TODO better order: expectSuggest("birkenh", [{name: "Birkenhain", type: "village"}, {name: "Birkenhof", type: "hamlet"}]);
    });
});
