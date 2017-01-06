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
    'hbs!tmpl/profile/ProfileColumnLayoutView_tmpl',
    'collection/VProfileList',
    'utils/Utils',
    'utils/Messages',
    'utils/Globals',
    'moment',
    'models/VEntity',
    'nvd3'
], function(require, Backbone, ProfileColumnLayoutViewTmpl, VProfileList, Utils, Messages, Globals, moment, VEntity) {
    'use strict';

    var ProfileColumnLayoutView = Backbone.Marionette.LayoutView.extend(
        /** @lends ProfileColumnLayoutView */
        {
            _viewName: 'ProfileColumnLayoutView',

            template: ProfileColumnLayoutViewTmpl,

            /** Layout sub regions */
            regions: {},
            templateHelpers: function() {
                return {
                    profileData: this.profileData.values ? this.profileData.values : this.profileData,
                    entityDetail: this.entityDetail,
                    systemAttributes: this.systemAttributes,
                    typeObject: this.typeObject
                };
            },
            /** ui selector cache */
            ui: {
                "backToYear": '[data-id="backToYear"]'
            },
            /** ui events hash */
            events: function() {
                var events = {};
                events["click " + this.ui.backToYear] = 'backToYear';
                return events;
            },
            /**
             * intialize a new ProfileColumnLayoutView Layout
             * @constructs
             */
            initialize: function(options) {
                _.extend(this, _.pick(options, 'profileData', 'guid', 'entityDetail', 'systemAttributes'));
                if (this.systemAttributes) {
                    this.systemAttributes['formatedTime'] = this.systemAttributes.createdTime ? moment(this.systemAttributes.createdTime).format("LL") : "--";
                } else {
                    this.systemAttributes = {};
                    this.systemAttributes['formatedTime'] = "--";
                }
                this.typeObject = Utils.getProfileTabType(this.profileData.values);
                this.entityModel = new VEntity();
                this.formatValue = d3.format("2s")

            },
            fetchEntity: function(argument) {
                var that = this;
                this.entityModel.getEntity(this.entityDetail.table.id, {
                    success: function(data) {
                        if (data.definition.values.profileData && data.definition.values.profileData.values.rowCount) {
                            that.$('.rowValue').show();
                            that.$('.rowValue .graphval').html('<b>' + that.formatValue(data.definition.values.profileData.values.rowCount).replace('G', 'B') + '</b>');
                        }
                        if (data.definition.values) {
                            if (data.definition.id) {
                                that.$('.table_name .graphval').html('<b><a href="#!/detailPage/' + data.definition.id.id + "?profile=true" + '">' + data.definition.values.name + '</a></b>');
                            }
                            that.$('.table_created .graphval').html('<b>' + moment(data.definition.values.createTime).format("LL") + '</b>');
                        }
                    },
                    error: function(error, data, status) {},
                    complete: function() {}
                });
            },
            bindEvents: function() {},
            onRender: function() {
                this.renderGraph();
                this.fetchEntity();
                if (this.typeObject && this.typeObject.type === "date") {
                    this.$('svg').addClass('dateType');
                }

            },
            renderGraph: function(argument) {
                if (!this.typeObject) {
                    return;
                }
                var that = this,
                    profileData = this.profileData.values;
                this.data = [{
                    key: this.typeObject.label,
                    values: this.typeObject.actualObj || []
                }];
                nv.addGraph(function() {
                    that.chart = nv.models.multiBarChart()
                        .x(function(d) {
                            return d.value || d.year
                        }) //Specify the data accessors.
                        .y(function(d) {
                            return d.count
                        })
                        .height(220)
                        .stacked(false)
                        .showControls(false)
                        .reduceXTicks(false)
                        .staggerLabels(true)
                        .duration(1000)
                        .groupSpacing(0.6)
                        .wrapLabels(true)
                        .showLegend(false);
                    if (that.typeObject.type !== "date") {
                        that.chart.rotateLabels(-45);
                    }
                    that.chart.color(["#38BB9B"]);

                    that.chart.yAxis.tickFormat(function(d) {
                        return that.formatValue(d).replace('G', 'B');
                    });
                    that.chart.xAxis
                        .axisLabel(that.typeObject.xAxisLabel)
                        .axisLabelDistance(-185);
                    that.chart.yAxis
                        .axisLabel(that.typeObject.yAxisLabel)
                        .axisLabelDistance(-5);
                    that.chart.margin({
                        right: 30,
                        left: 80,
                        top: 20,
                        bottom: 60
                    });
                    if (that.typeObject.type === "date") {
                        that.chart.multibar.dispatch.elementClick = function(e) {
                            if (!that.monthsData) {
                                that.createMonthData(e.data.monthlyCounts)
                            }
                        }
                    }
                    that.svg = d3.select(that.$('svg')[0]).datum(that.data)
                    that.svg.transition().duration(0).call(that.chart);

                    nv.utils.windowResize(that.chart.update);

                    return that.chart;
                });

            },
            backToYear: function() {
                this.ui.backToYear.hide();
                this.monthsData = null;
                this.updateGraph(this.data);
            },
            createMonthData: function(data) {
                var monthsKey = {
                        "1": "Jan",
                        "2": "Feb",
                        "3": "Mar",
                        "4": "Apr",
                        "5": "May",
                        "6": "Jun",
                        "7": "Jul",
                        "8": "Aug",
                        "9": "Sep",
                        "10": "Oct",
                        "11": "Nov",
                        "12": "Dec"
                    },
                    i = 1;

                this.monthsData = [{
                    key: this.typeObject.label,
                    values: []
                }];

                while (i <= 12) {
                    this.monthsData[0].values.push({
                        value: monthsKey[i],
                        count: data[i] || 0
                    });
                    i++;
                }
                this.ui.backToYear.show();
                this.updateGraph(this.monthsData);

            },
            updateGraph: function(data) {
                this.svg.datum(data).transition().duration(0).call(this.chart);
            },
        });
    return ProfileColumnLayoutView;
});
