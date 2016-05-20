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
	/// <summary>Metric generated from a method, mostly used by annotation</summary>
	internal class MethodMetric : org.apache.hadoop.metrics2.lib.MutableMetric
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.lib.MethodMetric
			)));

		private readonly object obj;

		private readonly java.lang.reflect.Method method;

		private readonly org.apache.hadoop.metrics2.MetricsInfo info;

		private readonly org.apache.hadoop.metrics2.lib.MutableMetric impl;

		internal MethodMetric(object obj, java.lang.reflect.Method method, org.apache.hadoop.metrics2.MetricsInfo
			 info, org.apache.hadoop.metrics2.annotation.Metric.Type type)
		{
			this.obj = com.google.common.@base.Preconditions.checkNotNull(obj, "object");
			this.method = org.apache.hadoop.metrics2.util.Contracts.checkArg(method, method.getParameterTypes
				().Length == 0, "Metric method should have no arguments");
			this.method.setAccessible(true);
			this.info = com.google.common.@base.Preconditions.checkNotNull(info, "info");
			impl = newImpl(com.google.common.@base.Preconditions.checkNotNull(type, "metric type"
				));
		}

		private org.apache.hadoop.metrics2.lib.MutableMetric newImpl(org.apache.hadoop.metrics2.annotation.Metric.Type
			 metricType)
		{
			java.lang.Class resType = method.getReturnType();
			switch (metricType)
			{
				case org.apache.hadoop.metrics2.annotation.Metric.Type.COUNTER:
				{
					return newCounter(resType);
				}

				case org.apache.hadoop.metrics2.annotation.Metric.Type.GAUGE:
				{
					return newGauge(resType);
				}

				case org.apache.hadoop.metrics2.annotation.Metric.Type.DEFAULT:
				{
					return resType == Sharpen.Runtime.getClassForType(typeof(string)) ? newTag(resType
						) : newGauge(resType);
				}

				case org.apache.hadoop.metrics2.annotation.Metric.Type.TAG:
				{
					return newTag(resType);
				}

				default:
				{
					org.apache.hadoop.metrics2.util.Contracts.checkArg(metricType, false, "unsupported metric type"
						);
					return null;
				}
			}
		}

		internal virtual org.apache.hadoop.metrics2.lib.MutableMetric newCounter(java.lang.Class
			 type)
		{
			if (isInt(type) || isLong(type))
			{
				return new _MutableMetric_73(this, type);
			}
			throw new org.apache.hadoop.metrics2.MetricsException("Unsupported counter type: "
				 + type.getName());
		}

		private sealed class _MutableMetric_73 : org.apache.hadoop.metrics2.lib.MutableMetric
		{
			public _MutableMetric_73(MethodMetric _enclosing, java.lang.Class type)
			{
				this._enclosing = _enclosing;
				this.type = type;
			}

			public override void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder rb, 
				bool all)
			{
				try
				{
					object ret = this._enclosing.method.invoke(this._enclosing.obj, (object[])null);
					if (org.apache.hadoop.metrics2.lib.MethodMetric.isInt(type))
					{
						rb.addCounter(this._enclosing.info, ((int)ret));
					}
					else
					{
						rb.addCounter(this._enclosing.info, ((long)ret));
					}
				}
				catch (System.Exception ex)
				{
					org.apache.hadoop.metrics2.lib.MethodMetric.LOG.error("Error invoking method " + 
						this._enclosing.method.getName(), ex);
				}
			}

			private readonly MethodMetric _enclosing;

			private readonly java.lang.Class type;
		}

		internal static bool isInt(java.lang.Class type)
		{
			bool ret = type == Sharpen.Runtime.getClassForType(typeof(int)) || type == Sharpen.Runtime.getClassForType
				(typeof(int));
			return ret;
		}

		internal static bool isLong(java.lang.Class type)
		{
			return type == Sharpen.Runtime.getClassForType(typeof(long)) || type == Sharpen.Runtime.getClassForType
				(typeof(long));
		}

		internal static bool isFloat(java.lang.Class type)
		{
			return type == Sharpen.Runtime.getClassForType(typeof(float)) || type == Sharpen.Runtime.getClassForType
				(typeof(float));
		}

		internal static bool isDouble(java.lang.Class type)
		{
			return type == Sharpen.Runtime.getClassForType(typeof(double)) || type == Sharpen.Runtime.getClassForType
				(typeof(double));
		}

		internal virtual org.apache.hadoop.metrics2.lib.MutableMetric newGauge(java.lang.Class
			 t)
		{
			if (isInt(t) || isLong(t) || isFloat(t) || isDouble(t))
			{
				return new _MutableMetric_108(this, t);
			}
			throw new org.apache.hadoop.metrics2.MetricsException("Unsupported gauge type: " 
				+ t.getName());
		}

		private sealed class _MutableMetric_108 : org.apache.hadoop.metrics2.lib.MutableMetric
		{
			public _MutableMetric_108(MethodMetric _enclosing, java.lang.Class t)
			{
				this._enclosing = _enclosing;
				this.t = t;
			}

			public override void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder rb, 
				bool all)
			{
				try
				{
					object ret = this._enclosing.method.invoke(this._enclosing.obj, (object[])null);
					if (org.apache.hadoop.metrics2.lib.MethodMetric.isInt(t))
					{
						rb.addGauge(this._enclosing.info, ((int)ret));
					}
					else
					{
						if (org.apache.hadoop.metrics2.lib.MethodMetric.isLong(t))
						{
							rb.addGauge(this._enclosing.info, ((long)ret));
						}
						else
						{
							if (org.apache.hadoop.metrics2.lib.MethodMetric.isFloat(t))
							{
								rb.addGauge(this._enclosing.info, ((float)ret));
							}
							else
							{
								rb.addGauge(this._enclosing.info, ((double)ret));
							}
						}
					}
				}
				catch (System.Exception ex)
				{
					org.apache.hadoop.metrics2.lib.MethodMetric.LOG.error("Error invoking method " + 
						this._enclosing.method.getName(), ex);
				}
			}

			private readonly MethodMetric _enclosing;

			private readonly java.lang.Class t;
		}

		internal virtual org.apache.hadoop.metrics2.lib.MutableMetric newTag(java.lang.Class
			 resType)
		{
			if (resType == Sharpen.Runtime.getClassForType(typeof(string)))
			{
				return new _MutableMetric_128(this);
			}
			throw new org.apache.hadoop.metrics2.MetricsException("Unsupported tag type: " + 
				resType.getName());
		}

		private sealed class _MutableMetric_128 : org.apache.hadoop.metrics2.lib.MutableMetric
		{
			public _MutableMetric_128(MethodMetric _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder rb, 
				bool all)
			{
				try
				{
					object ret = this._enclosing.method.invoke(this._enclosing.obj, (object[])null);
					rb.tag(this._enclosing.info, (string)ret);
				}
				catch (System.Exception ex)
				{
					org.apache.hadoop.metrics2.lib.MethodMetric.LOG.error("Error invoking method " + 
						this._enclosing.method.getName(), ex);
				}
			}

			private readonly MethodMetric _enclosing;
		}

		public override void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder builder
			, bool all)
		{
			impl.snapshot(builder, all);
		}

		internal static org.apache.hadoop.metrics2.MetricsInfo metricInfo(java.lang.reflect.Method
			 method)
		{
			return org.apache.hadoop.metrics2.lib.Interns.info(nameFrom(method), "Metric for "
				 + method.getName());
		}

		internal static string nameFrom(java.lang.reflect.Method method)
		{
			string methodName = method.getName();
			if (methodName.StartsWith("get"))
			{
				return org.apache.commons.lang.StringUtils.capitalize(Sharpen.Runtime.substring(methodName
					, 3));
			}
			return org.apache.commons.lang.StringUtils.capitalize(methodName);
		}
	}
}
