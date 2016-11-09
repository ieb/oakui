/*jslint node: true */
"use strict";

/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/

(function() {
    var app = angular.module('oakui', [], function($httpProvider) {
        // Angular does JSON POSTS by default, Sling doesnt, 
        // Use x-www-form-urlencoded Content-Type
        $httpProvider.defaults.headers.post['Content-Type'] = 'application/x-www-form-urlencoded;charset=utf-8';

        /**
         * The workhorse; converts an object to x-www-form-urlencoded serialization.
         * @param {Object} obj
         * @return {String}
         */ 
        var param = function(obj) {
          var query = '', name, value, fullSubName, subName, subValue, innerObj, i;
            
          for(name in obj) {
            value = obj[name];
              
            if(value instanceof Array) {
              for(i=0; i<value.length; ++i) {
                subValue = value[i];
                fullSubName = name;
                innerObj = {};
                innerObj[fullSubName] = subValue;
                query += param(innerObj) + '&';
              }
            }
            else if(value instanceof Object) {
              for(subName in value) {
                subValue = value[subName];
                fullSubName = name + '[' + subName + ']';
                innerObj = {};
                innerObj[fullSubName] = subValue;
                query += param(innerObj) + '&';
              }
            }
            else if(value !== undefined && value !== null)
              query += encodeURIComponent(name) + '=' + encodeURIComponent(value) + '&';
          }
            
          return query.length ? query.substr(0, query.length - 1) : query;
        };

        // Override $http service's default transformRequest
        $httpProvider.defaults.transformRequest = [function(data) {
          return angular.isObject(data) && String(data) !== '[object File]' ? param(data) : data;
        }];
    });
    var testing = false;
    var getLuceneData = function($http,callback) {
        $http.get("oakui/lucene.json").success(
            function(data, status) {
                if(typeof data === 'string') {
                    data = JSON.parse(data);
                }
                console.log("Got ",data);
                callback(data);
            }).error(function(data, status, headers, config) {
                console.log("No indexes found on this server ",data, status);
            });
    };


    var extend = function(target, source) {
        for (var v in source) {
            if(source.hasOwnProperty(v)) {
                if (target[v] === undefined) {
                    target[v] = source[v];                    
                }
            } 
        }
        return target;
    }

    app.controller('OakuiController', [ '$scope', '$http', function($scope, $http){

        $scope.lucene = {};
        $scope.migFormNo = 1;
        $scope.outputDate = function(d) {
            return new Date(d) ;
        };
        $scope.outputAge = function(d) {
            var s = (new Date().getTime() - d)/1000;
            var d = Math.floor(s/(24*3600));
            s = s-d*24*3600;
            var h = Math.floor(s/(3600));
            s = s-h*3600;
            var m = Math.floor(s/(60));
            s = Math.floor(s-m*60);
            return d+"d"+h+"h"+m+"m"+s+"s";
        };
        $scope.outputSize = function(d) {
            if ( d < 8192 ) {
                return d+" bytes";
            } else if ( d < 1024*1024 ) {
                return (Math.floor(d/102.4)/10)+" KB";
            } else if (d < 1024*1024*1024 ) {
                return (Math.floor(d/(1024*102.4))/10)+" MB";
            } else {
                return (Math.floor(d/(2014*1024*102.4))/10)+" GB";
            }
        };
        $scope.outputGenerations = function(d) {
            var count = 0;
            for (var i = d.length - 1; i >= 0; i--) {
                if (d[i].name.startsWith("segments")) {
                    count++;
                }
            };
            return count;
        };

        $scope.fileOrder = function(v1, v2 ) {
            return 1;
        }

        $scope.isSegment = function(name) {
            return name !== undefined && name.startsWith("segments");
        }
        $scope.isNotSegment = function(name) {
            return name !== undefined &&  !name.startsWith("segments");
        }
        $scope.isPastCommit = function(name) {
            return name.startsWith("segments_");
        }
        $scope.isHeadCommit = function(name) {
            return (name === "segments.gen");
        }
        $scope.analyseCommit = function(path, index) {

            $http.get("oakui/"+path+".an.json").success(
                function(data, status) {
                    if(typeof data === 'string') {
                        data = JSON.parse(data);
                    }
                    var indexFiles = [];
                    if (index.fileByName === undefined) {
                        index.fileByName = {};
                        index.files.sort(function compare(a,b){
                            if ( a.name < b.name ) {
                                return -1;
                            }
                            if ( a.name > b.name) {
                                return 1;
                            }
                            return 0;
                        });
                        var n = 0;
                        for ( var i = 0; i < index.files.length; i++) {
                            if (index.files[i].name.startsWith("segments")) {
                                index.files[i].segment_sequence = n;
                                index.files[i].segment_class = "segment_class_"+n;
                                n++;
                            }
                            if (index.files[i].segments === undefined) {
                                index.files[i].segments = [];
                            }
                            index.fileByName[index.files[i].name] = index.files[i];
                        }
                    }


                    for (var v in data) {
                        if(data.hasOwnProperty(v)) {
                            var segmentInfo = data[v];
                            var segmentClass = "segment_class_"+segmentInfo.segment_sequence;
                            var segmentName = segmentInfo.segment_name;
                            var files = {};
                            // iterate through the commit info and add the segment to the file.
                            for (var i = 0; i < segmentInfo.commits.length; i++) {
                               for ( var j = 0; j < segmentInfo.commits[i].files.length; j++) {
                                    var segList = index.fileByName[segmentInfo.commits[i].files[j]].segments;
                                    var got = false;
                                    for ( var k = 0; k < segList.length; k++ ) {
                                        if (segList[k].name === segmentName) {
                                           got = true;
                                        }
                                    }
                                    if ( !got ) {
                                        segList.push({
                                            name : segmentName,
                                            class : segmentClass
                                        });
                                    }
                               }
                            }
                        }
                    }
                }).error(function(data, status, headers, config) {
                    console.log("Analysis of segment failed ",data, status);
                });
        }

        $scope.revertCommit = function(path, name) {
            $http.post("oakui/"+path+"/"+name+".re.json").success(
                function(data, status) {
                    if(typeof data === 'string') {
                        data = JSON.parse(data);
                    }
                    console.log("Got ",data);
                }).error(function(data, status, headers, config) {
                    console.log("Revert of segment failed ",data, status);
                });

        }
        $scope.damageFile = function(path, name) {
            $http.post("oakui/"+path+"/"+name+".da.json").success(
                function(data, status) {
                    if(typeof data === 'string') {
                        data = JSON.parse(data);
                    }
                    console.log("Got ",data);
                }).error(function(data, status, headers, config) {
                    console.log("Damage of file failed ",data, status);
                });


        }
        /*
        $scope.download = function(path, name) {
            console.log("Download",path,name);
            $http.get("oakui/"+path+"/"+name+".do.json").success(
                function(data, status) {
                    if(typeof data === 'string') {
                        data = JSON.parse(data);
                    }
                    console.log("Got ",data);
                }).error(function(data, status, headers, config) {
                    console.log("Download of segment failed ",data, status);
                });

        }
        */


        $scope.reload = function() {
            getLuceneData($http,function(jsonRecords) {
                 $scope.lucene = jsonRecords;
                console.log("Loading finished ", $scope);
            });
        }
        // reload for the first time.
        $scope.reload();

        
    }]);   
}());