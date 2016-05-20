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

namespace org.apache.hadoop.classification.tools
{
	/// <summary>
	/// Process the
	/// <see cref="com.sun.javadoc.RootDoc"/>
	/// by substituting with (nested) proxy objects that
	/// exclude elements with Private or LimitedPrivate annotations.
	/// <p>
	/// Based on code from http://www.sixlegs.com/blog/java/exclude-javadoc-tag.html.
	/// </summary>
	internal class RootDocProcessor
	{
		internal static string stability = org.apache.hadoop.classification.tools.StabilityOptions
			.UNSTABLE_OPTION;

		internal static bool treatUnannotatedClassesAsPrivate = false;

		public static com.sun.javadoc.RootDoc process(com.sun.javadoc.RootDoc root)
		{
			return (com.sun.javadoc.RootDoc)process(root, Sharpen.Runtime.getClassForType(typeof(
				com.sun.javadoc.RootDoc)));
		}

		private static object process(object obj, java.lang.Class type)
		{
			if (obj == null)
			{
				return null;
			}
			java.lang.Class cls = Sharpen.Runtime.getClassForObject(obj);
			if (cls.getName().StartsWith("com.sun."))
			{
				return getProxy(obj);
			}
			else
			{
				if (obj is object[])
				{
					java.lang.Class componentType = type.isArray() ? type.getComponentType() : cls.getComponentType
						();
					object[] array = (object[])obj;
					object[] newArray = (object[])java.lang.reflect.Array.newInstance(componentType, 
						array.Length);
					for (int i = 0; i < array.Length; ++i)
					{
						newArray[i] = process(array[i], componentType);
					}
					return newArray;
				}
			}
			return obj;
		}

		private static System.Collections.Generic.IDictionary<object, object> proxies = new 
			java.util.WeakHashMap<object, object>();

		private static object getProxy(object obj)
		{
			object proxy = proxies[obj];
			if (proxy == null)
			{
				proxy = java.lang.reflect.Proxy.newProxyInstance(Sharpen.Runtime.getClassForObject
					(obj).getClassLoader(), Sharpen.Runtime.getClassForObject(obj).getInterfaces(), 
					new org.apache.hadoop.classification.tools.RootDocProcessor.ExcludeHandler(obj));
				proxies[obj] = proxy;
			}
			return proxy;
		}

		private class ExcludeHandler : java.lang.reflect.InvocationHandler
		{
			private object target;

			public ExcludeHandler(object target)
			{
				this.target = target;
			}

			/// <exception cref="System.Exception"/>
			public virtual object invoke(object proxy, java.lang.reflect.Method method, object
				[] args)
			{
				string methodName = method.getName();
				if (target is com.sun.javadoc.Doc)
				{
					if (methodName.Equals("isIncluded"))
					{
						com.sun.javadoc.Doc doc = (com.sun.javadoc.Doc)target;
						return !exclude(doc) && doc.isIncluded();
					}
					if (target is com.sun.javadoc.RootDoc)
					{
						if (methodName.Equals("classes"))
						{
							return filter(((com.sun.javadoc.RootDoc)target).classes(), Sharpen.Runtime.getClassForType
								(typeof(com.sun.javadoc.ClassDoc)));
						}
						else
						{
							if (methodName.Equals("specifiedClasses"))
							{
								return filter(((com.sun.javadoc.RootDoc)target).specifiedClasses(), Sharpen.Runtime.getClassForType
									(typeof(com.sun.javadoc.ClassDoc)));
							}
							else
							{
								if (methodName.Equals("specifiedPackages"))
								{
									return filter(((com.sun.javadoc.RootDoc)target).specifiedPackages(), Sharpen.Runtime.getClassForType
										(typeof(com.sun.javadoc.PackageDoc)));
								}
							}
						}
					}
					else
					{
						if (target is com.sun.javadoc.ClassDoc)
						{
							if (isFiltered(args))
							{
								if (methodName.Equals("methods"))
								{
									return filter(((com.sun.javadoc.ClassDoc)target).methods(true), Sharpen.Runtime.getClassForType
										(typeof(com.sun.javadoc.MethodDoc)));
								}
								else
								{
									if (methodName.Equals("fields"))
									{
										return filter(((com.sun.javadoc.ClassDoc)target).fields(true), Sharpen.Runtime.getClassForType
											(typeof(com.sun.javadoc.FieldDoc)));
									}
									else
									{
										if (methodName.Equals("innerClasses"))
										{
											return filter(((com.sun.javadoc.ClassDoc)target).innerClasses(true), Sharpen.Runtime.getClassForType
												(typeof(com.sun.javadoc.ClassDoc)));
										}
										else
										{
											if (methodName.Equals("constructors"))
											{
												return filter(((com.sun.javadoc.ClassDoc)target).constructors(true), Sharpen.Runtime.getClassForType
													(typeof(com.sun.javadoc.ConstructorDoc)));
											}
										}
									}
								}
							}
						}
						else
						{
							if (target is com.sun.javadoc.PackageDoc)
							{
								if (methodName.Equals("allClasses"))
								{
									if (isFiltered(args))
									{
										return filter(((com.sun.javadoc.PackageDoc)target).allClasses(true), Sharpen.Runtime.getClassForType
											(typeof(com.sun.javadoc.ClassDoc)));
									}
									else
									{
										return filter(((com.sun.javadoc.PackageDoc)target).allClasses(), Sharpen.Runtime.getClassForType
											(typeof(com.sun.javadoc.ClassDoc)));
									}
								}
								else
								{
									if (methodName.Equals("annotationTypes"))
									{
										return filter(((com.sun.javadoc.PackageDoc)target).annotationTypes(), Sharpen.Runtime.getClassForType
											(typeof(com.sun.javadoc.AnnotationTypeDoc)));
									}
									else
									{
										if (methodName.Equals("enums"))
										{
											return filter(((com.sun.javadoc.PackageDoc)target).enums(), Sharpen.Runtime.getClassForType
												(typeof(com.sun.javadoc.ClassDoc)));
										}
										else
										{
											if (methodName.Equals("errors"))
											{
												return filter(((com.sun.javadoc.PackageDoc)target).errors(), Sharpen.Runtime.getClassForType
													(typeof(com.sun.javadoc.ClassDoc)));
											}
											else
											{
												if (methodName.Equals("exceptions"))
												{
													return filter(((com.sun.javadoc.PackageDoc)target).exceptions(), Sharpen.Runtime.getClassForType
														(typeof(com.sun.javadoc.ClassDoc)));
												}
												else
												{
													if (methodName.Equals("interfaces"))
													{
														return filter(((com.sun.javadoc.PackageDoc)target).interfaces(), Sharpen.Runtime.getClassForType
															(typeof(com.sun.javadoc.ClassDoc)));
													}
													else
													{
														if (methodName.Equals("ordinaryClasses"))
														{
															return filter(((com.sun.javadoc.PackageDoc)target).ordinaryClasses(), Sharpen.Runtime.getClassForType
																(typeof(com.sun.javadoc.ClassDoc)));
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				if (args != null)
				{
					if (methodName.Equals("compareTo") || methodName.Equals("equals") || methodName.Equals
						("overrides") || methodName.Equals("subclassOf"))
					{
						args[0] = unwrap(args[0]);
					}
				}
				try
				{
					return process(method.invoke(target, args), method.getReturnType());
				}
				catch (java.lang.reflect.InvocationTargetException e)
				{
					throw e.getTargetException();
				}
			}

			private static bool exclude(com.sun.javadoc.Doc doc)
			{
				com.sun.javadoc.AnnotationDesc[] annotations = null;
				if (doc is com.sun.javadoc.ProgramElementDoc)
				{
					annotations = ((com.sun.javadoc.ProgramElementDoc)doc).annotations();
				}
				else
				{
					if (doc is com.sun.javadoc.PackageDoc)
					{
						annotations = ((com.sun.javadoc.PackageDoc)doc).annotations();
					}
				}
				if (annotations != null)
				{
					foreach (com.sun.javadoc.AnnotationDesc annotation in annotations)
					{
						string qualifiedTypeName = annotation.annotationType().qualifiedTypeName();
						if (qualifiedTypeName.Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.classification.InterfaceAudience.Private
							)).getCanonicalName()) || qualifiedTypeName.Equals(Sharpen.Runtime.getClassForType
							(typeof(org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate)).getCanonicalName
							()))
						{
							return true;
						}
						if (stability.Equals(org.apache.hadoop.classification.tools.StabilityOptions.EVOLVING_OPTION
							))
						{
							if (qualifiedTypeName.Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.classification.InterfaceStability.Unstable
								)).getCanonicalName()))
							{
								return true;
							}
						}
						if (stability.Equals(org.apache.hadoop.classification.tools.StabilityOptions.STABLE_OPTION
							))
						{
							if (qualifiedTypeName.Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.classification.InterfaceStability.Unstable
								)).getCanonicalName()) || qualifiedTypeName.Equals(Sharpen.Runtime.getClassForType
								(typeof(org.apache.hadoop.classification.InterfaceStability.Evolving)).getCanonicalName
								()))
							{
								return true;
							}
						}
					}
					foreach (com.sun.javadoc.AnnotationDesc annotation_1 in annotations)
					{
						string qualifiedTypeName = annotation_1.annotationType().qualifiedTypeName();
						if (qualifiedTypeName.Equals(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.classification.InterfaceAudience.Public
							)).getCanonicalName()))
						{
							return false;
						}
					}
				}
				if (treatUnannotatedClassesAsPrivate)
				{
					return doc.isClass() || doc.isInterface() || doc.isAnnotationType();
				}
				return false;
			}

			private static object[] filter(com.sun.javadoc.Doc[] array, java.lang.Class componentType
				)
			{
				if (array == null || array.Length == 0)
				{
					return array;
				}
				System.Collections.Generic.IList<object> list = new System.Collections.Generic.List
					<object>(array.Length);
				foreach (com.sun.javadoc.Doc entry in array)
				{
					if (!exclude(entry))
					{
						list.add(process(entry, componentType));
					}
				}
				return Sharpen.Collections.ToArray(list, (object[])java.lang.reflect.Array.newInstance
					(componentType, list.Count));
			}

			private object unwrap(object proxy)
			{
				if (proxy is java.lang.reflect.Proxy)
				{
					return ((org.apache.hadoop.classification.tools.RootDocProcessor.ExcludeHandler)java.lang.reflect.Proxy
						.getInvocationHandler(proxy)).target;
				}
				return proxy;
			}

			private bool isFiltered(object[] args)
			{
				return args != null && true.Equals(args[0]);
			}
		}
	}
}
