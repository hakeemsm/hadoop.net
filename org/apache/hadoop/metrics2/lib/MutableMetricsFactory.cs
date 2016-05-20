/*
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
using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	public class MutableMetricsFactory
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableMetricsFactory
			)));

		internal virtual org.apache.hadoop.metrics2.lib.MutableMetric newForField(java.lang.reflect.Field
			 field, org.apache.hadoop.metrics2.annotation.Metric annotation, org.apache.hadoop.metrics2.lib.MetricsRegistry
			 registry)
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("field " + field + " with annotation " + annotation);
			}
			org.apache.hadoop.metrics2.MetricsInfo info = getInfo(annotation, field);
			org.apache.hadoop.metrics2.lib.MutableMetric metric = newForField(field, annotation
				);
			if (metric != null)
			{
				registry.add(info.name(), metric);
				return metric;
			}
			java.lang.Class cls = field.getType();
			if (cls == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableCounterInt
				)))
			{
				return registry.newCounter(info, 0);
			}
			if (cls == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableCounterLong
				)))
			{
				return registry.newCounter(info, 0L);
			}
			if (cls == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableGaugeInt
				)))
			{
				return registry.newGauge(info, 0);
			}
			if (cls == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableGaugeLong
				)))
			{
				return registry.newGauge(info, 0L);
			}
			if (cls == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableRate
				)))
			{
				return registry.newRate(info.name(), info.description(), annotation.always());
			}
			if (cls == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableRates
				)))
			{
				return new org.apache.hadoop.metrics2.lib.MutableRates(registry);
			}
			if (cls == Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MutableStat
				)))
			{
				return registry.newStat(info.name(), info.description(), annotation.sampleName(), 
					annotation.valueName(), annotation.always());
			}
			throw new org.apache.hadoop.metrics2.MetricsException("Unsupported metric field "
				 + field.getName() + " of type " + field.getType().getName());
		}

		internal virtual org.apache.hadoop.metrics2.lib.MutableMetric newForMethod(object
			 source, java.lang.reflect.Method method, org.apache.hadoop.metrics2.annotation.Metric
			 annotation, org.apache.hadoop.metrics2.lib.MetricsRegistry registry)
		{
			if (LOG.isDebugEnabled())
			{
				LOG.debug("method " + method + " with annotation " + annotation);
			}
			org.apache.hadoop.metrics2.MetricsInfo info = getInfo(annotation, method);
			org.apache.hadoop.metrics2.lib.MutableMetric metric = newForMethod(source, method
				, annotation);
			metric = metric != null ? metric : new org.apache.hadoop.metrics2.lib.MethodMetric
				(source, method, info, annotation.type());
			registry.add(info.name(), metric);
			return metric;
		}

		/// <summary>Override to handle custom mutable metrics for fields</summary>
		/// <param name="field">of the metric</param>
		/// <param name="annotation">of the field</param>
		/// <returns>a new metric object or null</returns>
		protected internal virtual org.apache.hadoop.metrics2.lib.MutableMetric newForField
			(java.lang.reflect.Field field, org.apache.hadoop.metrics2.annotation.Metric annotation
			)
		{
			return null;
		}

		/// <summary>Override to handle custom mutable metrics for methods</summary>
		/// <param name="source">the metrics source object</param>
		/// <param name="method">to return the metric</param>
		/// <param name="annotation">of the method</param>
		/// <returns>a new metric object or null</returns>
		protected internal virtual org.apache.hadoop.metrics2.lib.MutableMetric newForMethod
			(object source, java.lang.reflect.Method method, org.apache.hadoop.metrics2.annotation.Metric
			 annotation)
		{
			return null;
		}

		protected internal virtual org.apache.hadoop.metrics2.MetricsInfo getInfo(org.apache.hadoop.metrics2.annotation.Metric
			 annotation, java.lang.reflect.Field field)
		{
			return getInfo(annotation, getName(field));
		}

		protected internal virtual string getName(java.lang.reflect.Field field)
		{
			return org.apache.commons.lang.StringUtils.capitalize(field.getName());
		}

		protected internal virtual org.apache.hadoop.metrics2.MetricsInfo getInfo(org.apache.hadoop.metrics2.annotation.Metric
			 annotation, java.lang.reflect.Method method)
		{
			return getInfo(annotation, getName(method));
		}

		protected internal virtual org.apache.hadoop.metrics2.MetricsInfo getInfo(java.lang.Class
			 cls, org.apache.hadoop.metrics2.annotation.Metrics annotation)
		{
			string name = annotation.name();
			string about = annotation.about();
			string name2 = name.isEmpty() ? cls.getSimpleName() : name;
			return org.apache.hadoop.metrics2.lib.Interns.info(name2, about.isEmpty() ? name2
				 : about);
		}

		protected internal virtual string getName(java.lang.reflect.Method method)
		{
			string methodName = method.getName();
			if (methodName.StartsWith("get"))
			{
				return org.apache.commons.lang.StringUtils.capitalize(Sharpen.Runtime.substring(methodName
					, 3));
			}
			return org.apache.commons.lang.StringUtils.capitalize(methodName);
		}

		protected internal virtual org.apache.hadoop.metrics2.MetricsInfo getInfo(org.apache.hadoop.metrics2.annotation.Metric
			 annotation, string defaultName)
		{
			string[] value = annotation.value();
			if (value.Length == 2)
			{
				return org.apache.hadoop.metrics2.lib.Interns.info(value[0], value[1]);
			}
			if (value.Length == 1)
			{
				return org.apache.hadoop.metrics2.lib.Interns.info(defaultName, value[0]);
			}
			return org.apache.hadoop.metrics2.lib.Interns.info(defaultName, defaultName);
		}
	}
}
