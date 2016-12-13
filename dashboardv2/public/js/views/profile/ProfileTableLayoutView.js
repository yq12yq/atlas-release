/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

define(['require',
    'backbone',
    'hbs!tmpl/profile/ProfileTableLayoutView_tmpl',
    'collection/VProfileList',
    'utils/Utils',
    'utils/Messages',
    'utils/Globals',
    'collection/VCommonList',
    'sparkline'
], function(require, Backbone, ProfileTableLayoutViewTmpl, VProfileList, Utils, Messages, Globals, VCommonList, sparkline) {
    'use strict';

    var ProfileTableLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends ProfileTableLayoutView */
        {
            _viewName: 'ProfileTableLayoutView',

            template: ProfileTableLayoutViewTmpl,

            /** Layout sub regions */
            regions: {
                RProfileTableLayoutView: '#r_profileTableLayoutView'
            },
            /** ui selector cache */
            ui: {},
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.addTag] = 'checkedValue';
                return events;
            },
            /**
             * intialize a new ProfileTableLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'profileData', 'guid', 'entityDetail', 'systemAttributes'));
                var that = this;
                this.profileCollection = new VCommonList([], {});
                _.each(this.entityDetail.columns, function(obj) {
                    if(obj.values.profileData !== null){
                      that.profileCollection.add(_.extend({}, obj.values, obj.values.profileData.values, obj.id));   
                    }
                });
            },
            onRender: function() {
                this.renderTableLayoutView();
            },
            renderTableLayoutView: function() {
                var that = this;
                require(['utils/TableLayout'], function(TableLayout) {
                    var cols = new Backgrid.Columns(that.getAuditTableColumns());
                    that.RProfileTableLayoutView.show(new TableLayout(_.extend({}, {
                        columns: cols,
                        collection: that.profileCollection,
                        includeFilter: false,
                        includePagination: false,
                        includePageSize: false,
                        includeFooterRecords: false,
                        gridOpts: {
                            className: "table table-hover backgrid table-quickMenu",
                            emptyText: 'No records found!'
                        }
                    })));
                    that.renderGraphs();
                });
            },
            renderGraphs: function() {
                this.$('.sparklines').sparkline('html', { enableTagOptions: true });
                this.$('.sparklines').bind('sparklineClick', function(ev) {
                    var id = $(ev.target).data().guid;
                    Utils.setUrl({
                        url: '#!/detailPage/' + id,
                        mergeBrowserUrl: false,
                        trigger: true,
                        urlParams: {
                            'profile': true
                        }
                    });
                });
            },
            getAuditTableColumns: function() {
                var that = this;
                return this.profileCollection.constructor.getTableCols({
                    name: {
                        label: "Name",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return '<div><a href="#!/detailPage/' + model.get('id') + '?profile=true">' + rawValue + '</a></div>';
                            }
                        })
                    },
                    type: {
                        label: "Type",
                        cell: "String",
                        editable: false,
                        sortable: false,
                    },
                    nonNullData: {
                        label: "Data",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                return rawValue + "%"
                            }
                        })
                    },

                    distributionDecile: {
                        label: "Distribution",
                        cell: "Html",
                        editable: false,
                        sortable: false,
                        formatter: _.extend({}, Backgrid.CellFormatter.prototype, {
                            fromRaw: function(rawValue, model) {
                                var sparkarray = [];
                                var distibutionObj = Utils.getProfileTabType(model.toJSON());
                                if (distibutionObj) {
                                    _.each(distibutionObj.actualObj, function(obj) {
                                        sparkarray.push(obj.count);
                                    })
                                }

                                return '<span data-guid="' + model.get('id') + '" class="sparklines" sparkType="bar" sparkBarColor="#38BB9B" values="' + sparkarray.join(',') + '"></span>'
                            }
                        })
                    },
                    cardinality: {
                        label: "Cardinality",
                        cell: "String",
                        editable: false,
                        sortable: false,
                    },
                    minValue: {
                        label: "Min",
                        cell: "String",
                        editable: false,
                        sortable: false,
                    },
                    maxValue: {
                        label: "Max",
                        cell: "String",
                        editable: false,
                        sortable: false,
                    },
                    averageLength: {
                        label: "Average Length",
                        cell: "String",
                        editable: false,
                        sortable: false,
                    },
                    maxLength: {
                        label: "Max Length",
                        cell: "String",
                        editable: false,
                        sortable: false,
                    },
                }, this.profileCollection);

            }

        });
    return ProfileTableLayoutView;
});
