using System.Collections.Generic;
using System.IO;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Record.Compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JRecord : JCompType
	{
		internal class JavaRecord : JCompType.JavaCompType
		{
			private string fullName;

			private string name;

			private string module;

			private AList<JField<JType.JavaType>> fields = new AList<JField<JType.JavaType>>(
				);

			internal JavaRecord(JRecord _enclosing, string name, AList<JField<JType>> flist)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.fullName = name;
				int idx = name.LastIndexOf('.');
				this.name = Sharpen.Runtime.Substring(name, idx + 1);
				this.module = Sharpen.Runtime.Substring(name, 0, idx);
				for (IEnumerator<JField<JType>> iter = flist.GetEnumerator(); iter.HasNext(); )
				{
					JField<JType> f = iter.Next();
					this.fields.AddItem(new JField<JType.JavaType>(f.GetName(), f.GetType().GetJavaType
						()));
				}
			}

			internal override string GetTypeIDObjectString()
			{
				return "new org.apache.hadoop.record.meta.StructTypeID(" + this.fullName + ".getTypeInfo())";
			}

			internal override void GenSetRTIFilter(CodeBuffer cb, IDictionary<string, int> nestedStructMap
				)
			{
				// ignore, if we'ev already set the type filter for this record
				if (!nestedStructMap.Contains(this.fullName))
				{
					// we set the RTI filter here
					cb.Append(this.fullName + ".setTypeFilter(rti.getNestedStructTypeInfo(\"" + this.
						name + "\"));\n");
					nestedStructMap[this.fullName] = null;
				}
			}

			// for each typeInfo in the filter, we see if there's a similar one in the record. 
			// Since we store typeInfos in ArrayLists, thsi search is O(n squared). We do it faster
			// if we also store a map (of TypeInfo to index), but since setupRtiFields() is called
			// only once when deserializing, we're sticking with the former, as the code is easier.  
			internal virtual void GenSetupRtiFields(CodeBuffer cb)
			{
				cb.Append("private static void setupRtiFields()\n{\n");
				cb.Append("if (null == " + Consts.RtiFilter + ") return;\n");
				cb.Append("// we may already have done this\n");
				cb.Append("if (null != " + Consts.RtiFilterFields + ") return;\n");
				cb.Append("int " + Consts.RioPrefix + "i, " + Consts.RioPrefix + "j;\n");
				cb.Append(Consts.RtiFilterFields + " = new int [" + Consts.RioPrefix + "rtiFilter.getFieldTypeInfos().size()];\n"
					);
				cb.Append("for (" + Consts.RioPrefix + "i=0; " + Consts.RioPrefix + "i<" + Consts
					.RtiFilterFields + ".length; " + Consts.RioPrefix + "i++) {\n");
				cb.Append(Consts.RtiFilterFields + "[" + Consts.RioPrefix + "i] = 0;\n");
				cb.Append("}\n");
				cb.Append("java.util.Iterator<org.apache.hadoop.record.meta." + "FieldTypeInfo> "
					 + Consts.RioPrefix + "itFilter = " + Consts.RioPrefix + "rtiFilter.getFieldTypeInfos().iterator();\n"
					);
				cb.Append(Consts.RioPrefix + "i=0;\n");
				cb.Append("while (" + Consts.RioPrefix + "itFilter.hasNext()) {\n");
				cb.Append("org.apache.hadoop.record.meta.FieldTypeInfo " + Consts.RioPrefix + "tInfoFilter = "
					 + Consts.RioPrefix + "itFilter.next();\n");
				cb.Append("java.util.Iterator<org.apache.hadoop.record.meta." + "FieldTypeInfo> "
					 + Consts.RioPrefix + "it = " + Consts.RtiVar + ".getFieldTypeInfos().iterator();\n"
					);
				cb.Append(Consts.RioPrefix + "j=1;\n");
				cb.Append("while (" + Consts.RioPrefix + "it.hasNext()) {\n");
				cb.Append("org.apache.hadoop.record.meta.FieldTypeInfo " + Consts.RioPrefix + "tInfo = "
					 + Consts.RioPrefix + "it.next();\n");
				cb.Append("if (" + Consts.RioPrefix + "tInfo.equals(" + Consts.RioPrefix + "tInfoFilter)) {\n"
					);
				cb.Append(Consts.RtiFilterFields + "[" + Consts.RioPrefix + "i] = " + Consts.RioPrefix
					 + "j;\n");
				cb.Append("break;\n");
				cb.Append("}\n");
				cb.Append(Consts.RioPrefix + "j++;\n");
				cb.Append("}\n");
				/*int ct = 0;
				for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
				ct++;
				JField<JavaType> jf = i.next();
				JavaType type = jf.getType();
				String name = jf.getName();
				if (ct != 1) {
				cb.append("else ");
				}
				type.genRtiFieldCondition(cb, name, ct);
				}
				if (ct != 0) {
				cb.append("else {\n");
				cb.append("rtiFilterFields[i] = 0;\n");
				cb.append("}\n");
				}*/
				cb.Append(Consts.RioPrefix + "i++;\n");
				cb.Append("}\n");
				cb.Append("}\n");
			}

			internal override void GenReadMethod(CodeBuffer cb, string fname, string tag, bool
				 decl)
			{
				if (decl)
				{
					cb.Append(this.fullName + " " + fname + ";\n");
				}
				cb.Append(fname + "= new " + this.fullName + "();\n");
				cb.Append(fname + ".deserialize(" + Consts.RecordInput + ",\"" + tag + "\");\n");
			}

			internal override void GenWriteMethod(CodeBuffer cb, string fname, string tag)
			{
				cb.Append(fname + ".serialize(" + Consts.RecordOutput + ",\"" + tag + "\");\n");
			}

			internal override void GenSlurpBytes(CodeBuffer cb, string b, string s, string l)
			{
				cb.Append("{\n");
				cb.Append("int r = " + this.fullName + ".Comparator.slurpRaw(" + b + "," + s + ","
					 + l + ");\n");
				cb.Append(s + "+=r; " + l + "-=r;\n");
				cb.Append("}\n");
			}

			internal override void GenCompareBytes(CodeBuffer cb)
			{
				cb.Append("{\n");
				cb.Append("int r1 = " + this.fullName + ".Comparator.compareRaw(b1,s1,l1,b2,s2,l2);\n"
					);
				cb.Append("if (r1 <= 0) { return r1; }\n");
				cb.Append("s1+=r1; s2+=r1; l1-=r1; l2-=r1;\n");
				cb.Append("}\n");
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void GenCode(string destDir, AList<string> options)
			{
				string pkg = this.module;
				string pkgpath = pkg.ReplaceAll("\\.", "/");
				FilePath pkgdir = new FilePath(destDir, pkgpath);
				FilePath jfile = new FilePath(pkgdir, this.name + ".java");
				if (!pkgdir.Exists())
				{
					// create the pkg directory
					bool ret = pkgdir.Mkdirs();
					if (!ret)
					{
						throw new IOException("Cannnot create directory: " + pkgpath);
					}
				}
				else
				{
					if (!pkgdir.IsDirectory())
					{
						// not a directory
						throw new IOException(pkgpath + " is not a directory.");
					}
				}
				CodeBuffer cb = new CodeBuffer();
				cb.Append("// File generated by hadoop record compiler. Do not edit.\n");
				cb.Append("package " + this.module + ";\n\n");
				cb.Append("public class " + this.name + " extends org.apache.hadoop.record.Record {\n"
					);
				// type information declarations
				cb.Append("private static final " + "org.apache.hadoop.record.meta.RecordTypeInfo "
					 + Consts.RtiVar + ";\n");
				cb.Append("private static " + "org.apache.hadoop.record.meta.RecordTypeInfo " + Consts
					.RtiFilter + ";\n");
				cb.Append("private static int[] " + Consts.RtiFilterFields + ";\n");
				// static init for type information
				cb.Append("static {\n");
				cb.Append(Consts.RtiVar + " = " + "new org.apache.hadoop.record.meta.RecordTypeInfo(\""
					 + this.name + "\");\n");
				for (IEnumerator<JField<JType.JavaType>> i = this.fields.GetEnumerator(); i.HasNext
					(); )
				{
					JField<JType.JavaType> jf = i.Next();
					string name = jf.GetName();
					JType.JavaType type = jf.GetType();
					type.GenStaticTypeInfo(cb, name);
				}
				cb.Append("}\n\n");
				// field definitions
				for (IEnumerator<JField<JType.JavaType>> i_1 = this.fields.GetEnumerator(); i_1.HasNext
					(); )
				{
					JField<JType.JavaType> jf = i_1.Next();
					string name = jf.GetName();
					JType.JavaType type = jf.GetType();
					type.GenDecl(cb, name);
				}
				// default constructor
				cb.Append("public " + this.name + "() { }\n");
				// constructor
				cb.Append("public " + this.name + "(\n");
				int fIdx = 0;
				for (IEnumerator<JField<JType.JavaType>> i_2 = this.fields.GetEnumerator(); i_2.HasNext
					(); fIdx++)
				{
					JField<JType.JavaType> jf = i_2.Next();
					string name = jf.GetName();
					JType.JavaType type = jf.GetType();
					type.GenConstructorParam(cb, name);
					cb.Append((!i_2.HasNext()) ? string.Empty : ",\n");
				}
				cb.Append(") {\n");
				fIdx = 0;
				for (IEnumerator<JField<JType.JavaType>> i_3 = this.fields.GetEnumerator(); i_3.HasNext
					(); fIdx++)
				{
					JField<JType.JavaType> jf = i_3.Next();
					string name = jf.GetName();
					JType.JavaType type = jf.GetType();
					type.GenConstructorSet(cb, name);
				}
				cb.Append("}\n");
				// getter/setter for type info
				cb.Append("public static org.apache.hadoop.record.meta.RecordTypeInfo" + " getTypeInfo() {\n"
					);
				cb.Append("return " + Consts.RtiVar + ";\n");
				cb.Append("}\n");
				cb.Append("public static void setTypeFilter(" + "org.apache.hadoop.record.meta.RecordTypeInfo rti) {\n"
					);
				cb.Append("if (null == rti) return;\n");
				cb.Append(Consts.RtiFilter + " = rti;\n");
				cb.Append(Consts.RtiFilterFields + " = null;\n");
				// set RTIFilter for nested structs.
				// To prevent setting up the type filter for the same struct more than once, 
				// we use a hash map to keep track of what we've set. 
				IDictionary<string, int> nestedStructMap = new Dictionary<string, int>();
				foreach (JField<JType.JavaType> jf_1 in this.fields)
				{
					JType.JavaType type = jf_1.GetType();
					type.GenSetRTIFilter(cb, nestedStructMap);
				}
				cb.Append("}\n");
				// setupRtiFields()
				this.GenSetupRtiFields(cb);
				// getters/setters for member variables
				for (IEnumerator<JField<JType.JavaType>> i_4 = this.fields.GetEnumerator(); i_4.HasNext
					(); )
				{
					JField<JType.JavaType> jf = i_4.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					type.GenGetSet(cb, name);
				}
				// serialize()
				cb.Append("public void serialize(" + "final org.apache.hadoop.record.RecordOutput "
					 + Consts.RecordOutput + ", final String " + Consts.Tag + ")\n" + "throws java.io.IOException {\n"
					);
				cb.Append(Consts.RecordOutput + ".startRecord(this," + Consts.Tag + ");\n");
				for (IEnumerator<JField<JType.JavaType>> i_5 = this.fields.GetEnumerator(); i_5.HasNext
					(); )
				{
					JField<JType.JavaType> jf = i_5.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					type.GenWriteMethod(cb, name, name);
				}
				cb.Append(Consts.RecordOutput + ".endRecord(this," + Consts.Tag + ");\n");
				cb.Append("}\n");
				// deserializeWithoutFilter()
				cb.Append("private void deserializeWithoutFilter(" + "final org.apache.hadoop.record.RecordInput "
					 + Consts.RecordInput + ", final String " + Consts.Tag + ")\n" + "throws java.io.IOException {\n"
					);
				cb.Append(Consts.RecordInput + ".startRecord(" + Consts.Tag + ");\n");
				for (IEnumerator<JField<JType.JavaType>> i_6 = this.fields.GetEnumerator(); i_6.HasNext
					(); )
				{
					JField<JType.JavaType> jf = i_6.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					type.GenReadMethod(cb, name, name, false);
				}
				cb.Append(Consts.RecordInput + ".endRecord(" + Consts.Tag + ");\n");
				cb.Append("}\n");
				// deserialize()
				cb.Append("public void deserialize(final " + "org.apache.hadoop.record.RecordInput "
					 + Consts.RecordInput + ", final String " + Consts.Tag + ")\n" + "throws java.io.IOException {\n"
					);
				cb.Append("if (null == " + Consts.RtiFilter + ") {\n");
				cb.Append("deserializeWithoutFilter(" + Consts.RecordInput + ", " + Consts.Tag + 
					");\n");
				cb.Append("return;\n");
				cb.Append("}\n");
				cb.Append("// if we're here, we need to read based on version info\n");
				cb.Append(Consts.RecordInput + ".startRecord(" + Consts.Tag + ");\n");
				cb.Append("setupRtiFields();\n");
				cb.Append("for (int " + Consts.RioPrefix + "i=0; " + Consts.RioPrefix + "i<" + Consts
					.RtiFilter + ".getFieldTypeInfos().size(); " + Consts.RioPrefix + "i++) {\n");
				int ct = 0;
				for (IEnumerator<JField<JType.JavaType>> i_7 = this.fields.GetEnumerator(); i_7.HasNext
					(); )
				{
					JField<JType.JavaType> jf = i_7.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					ct++;
					if (1 != ct)
					{
						cb.Append("else ");
					}
					cb.Append("if (" + ct + " == " + Consts.RtiFilterFields + "[" + Consts.RioPrefix 
						+ "i]) {\n");
					type.GenReadMethod(cb, name, name, false);
					cb.Append("}\n");
				}
				if (0 != ct)
				{
					cb.Append("else {\n");
					cb.Append("java.util.ArrayList<" + "org.apache.hadoop.record.meta.FieldTypeInfo> typeInfos = "
						 + "(java.util.ArrayList<" + "org.apache.hadoop.record.meta.FieldTypeInfo>)" + "("
						 + Consts.RtiFilter + ".getFieldTypeInfos());\n");
					cb.Append("org.apache.hadoop.record.meta.Utils.skip(" + Consts.RecordInput + ", "
						 + "typeInfos.get(" + Consts.RioPrefix + "i).getFieldID(), typeInfos.get(" + Consts
						.RioPrefix + "i).getTypeID());\n");
					cb.Append("}\n");
				}
				cb.Append("}\n");
				cb.Append(Consts.RecordInput + ".endRecord(" + Consts.Tag + ");\n");
				cb.Append("}\n");
				// compareTo()
				cb.Append("public int compareTo (final Object " + Consts.RioPrefix + "peer_) throws ClassCastException {\n"
					);
				cb.Append("if (!(" + Consts.RioPrefix + "peer_ instanceof " + this.name + ")) {\n"
					);
				cb.Append("throw new ClassCastException(\"Comparing different types of records.\");\n"
					);
				cb.Append("}\n");
				cb.Append(this.name + " " + Consts.RioPrefix + "peer = (" + this.name + ") " + Consts
					.RioPrefix + "peer_;\n");
				cb.Append("int " + Consts.RioPrefix + "ret = 0;\n");
				for (IEnumerator<JField<JType.JavaType>> i_8 = this.fields.GetEnumerator(); i_8.HasNext
					(); )
				{
					JField<JType.JavaType> jf = i_8.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					type.GenCompareTo(cb, name, Consts.RioPrefix + "peer." + name);
					cb.Append("if (" + Consts.RioPrefix + "ret != 0) return " + Consts.RioPrefix + "ret;\n"
						);
				}
				cb.Append("return " + Consts.RioPrefix + "ret;\n");
				cb.Append("}\n");
				// equals()
				cb.Append("public boolean equals(final Object " + Consts.RioPrefix + "peer_) {\n"
					);
				cb.Append("if (!(" + Consts.RioPrefix + "peer_ instanceof " + this.name + ")) {\n"
					);
				cb.Append("return false;\n");
				cb.Append("}\n");
				cb.Append("if (" + Consts.RioPrefix + "peer_ == this) {\n");
				cb.Append("return true;\n");
				cb.Append("}\n");
				cb.Append(this.name + " " + Consts.RioPrefix + "peer = (" + this.name + ") " + Consts
					.RioPrefix + "peer_;\n");
				cb.Append("boolean " + Consts.RioPrefix + "ret = false;\n");
				for (IEnumerator<JField<JType.JavaType>> i_9 = this.fields.GetEnumerator(); i_9.HasNext
					(); )
				{
					JField<JType.JavaType> jf = i_9.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					type.GenEquals(cb, name, Consts.RioPrefix + "peer." + name);
					cb.Append("if (!" + Consts.RioPrefix + "ret) return " + Consts.RioPrefix + "ret;\n"
						);
				}
				cb.Append("return " + Consts.RioPrefix + "ret;\n");
				cb.Append("}\n");
				// clone()
				cb.Append("public Object clone() throws CloneNotSupportedException {\n");
				cb.Append(this.name + " " + Consts.RioPrefix + "other = new " + this.name + "();\n"
					);
				for (IEnumerator<JField<JType.JavaType>> i_10 = this.fields.GetEnumerator(); i_10
					.HasNext(); )
				{
					JField<JType.JavaType> jf = i_10.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					type.GenClone(cb, name);
				}
				cb.Append("return " + Consts.RioPrefix + "other;\n");
				cb.Append("}\n");
				cb.Append("public int hashCode() {\n");
				cb.Append("int " + Consts.RioPrefix + "result = 17;\n");
				cb.Append("int " + Consts.RioPrefix + "ret;\n");
				for (IEnumerator<JField<JType.JavaType>> i_11 = this.fields.GetEnumerator(); i_11
					.HasNext(); )
				{
					JField<JType.JavaType> jf = i_11.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					type.GenHashCode(cb, name);
					cb.Append(Consts.RioPrefix + "result = 37*" + Consts.RioPrefix + "result + " + Consts
						.RioPrefix + "ret;\n");
				}
				cb.Append("return " + Consts.RioPrefix + "result;\n");
				cb.Append("}\n");
				cb.Append("public static String signature() {\n");
				cb.Append("return \"" + this._enclosing.GetSignature() + "\";\n");
				cb.Append("}\n");
				cb.Append("public static class Comparator extends" + " org.apache.hadoop.record.RecordComparator {\n"
					);
				cb.Append("public Comparator() {\n");
				cb.Append("super(" + this.name + ".class);\n");
				cb.Append("}\n");
				cb.Append("static public int slurpRaw(byte[] b, int s, int l) {\n");
				cb.Append("try {\n");
				cb.Append("int os = s;\n");
				for (IEnumerator<JField<JType.JavaType>> i_12 = this.fields.GetEnumerator(); i_12
					.HasNext(); )
				{
					JField<JType.JavaType> jf = i_12.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					type.GenSlurpBytes(cb, "b", "s", "l");
				}
				cb.Append("return (os - s);\n");
				cb.Append("} catch(java.io.IOException e) {\n");
				cb.Append("throw new RuntimeException(e);\n");
				cb.Append("}\n");
				cb.Append("}\n");
				cb.Append("static public int compareRaw(byte[] b1, int s1, int l1,\n");
				cb.Append("                             byte[] b2, int s2, int l2) {\n");
				cb.Append("try {\n");
				cb.Append("int os1 = s1;\n");
				for (IEnumerator<JField<JType.JavaType>> i_13 = this.fields.GetEnumerator(); i_13
					.HasNext(); )
				{
					JField<JType.JavaType> jf = i_13.Next();
					string name = jf_1.GetName();
					JType.JavaType type = jf_1.GetType();
					type.GenCompareBytes(cb);
				}
				cb.Append("return (os1 - s1);\n");
				cb.Append("} catch(java.io.IOException e) {\n");
				cb.Append("throw new RuntimeException(e);\n");
				cb.Append("}\n");
				cb.Append("}\n");
				cb.Append("public int compare(byte[] b1, int s1, int l1,\n");
				cb.Append("                   byte[] b2, int s2, int l2) {\n");
				cb.Append("int ret = compareRaw(b1,s1,l1,b2,s2,l2);\n");
				cb.Append("return (ret == -1)? -1 : ((ret==0)? 1 : 0);");
				cb.Append("}\n");
				cb.Append("}\n\n");
				cb.Append("static {\n");
				cb.Append("org.apache.hadoop.record.RecordComparator.define(" + this.name + ".class, new Comparator());\n"
					);
				cb.Append("}\n");
				cb.Append("}\n");
				FileWriter jj = new FileWriter(jfile);
				try
				{
					jj.Write(cb.ToString());
				}
				finally
				{
					jj.Close();
				}
			}

			private readonly JRecord _enclosing;
		}

		internal class CppRecord : JCompType.CppCompType
		{
			private string fullName;

			private string name;

			private string module;

			private AList<JField<JType.CppType>> fields = new AList<JField<JType.CppType>>();

			internal CppRecord(JRecord _enclosing, string name, AList<JField<JType>> flist)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.fullName = name.ReplaceAll("\\.", "::");
				int idx = name.LastIndexOf('.');
				this.name = Sharpen.Runtime.Substring(name, idx + 1);
				this.module = Sharpen.Runtime.Substring(name, 0, idx).ReplaceAll("\\.", "::");
				for (IEnumerator<JField<JType>> iter = flist.GetEnumerator(); iter.HasNext(); )
				{
					JField<JType> f = iter.Next();
					this.fields.AddItem(new JField<JType.CppType>(f.GetName(), f.GetType().GetCppType
						()));
				}
			}

			internal override string GetTypeIDObjectString()
			{
				return "new ::hadoop::StructTypeID(" + this.fullName + "::getTypeInfo().getFieldTypeInfos())";
			}

			internal virtual string GenDecl(string fname)
			{
				return "  " + this.name + " " + fname + ";\n";
			}

			internal override void GenSetRTIFilter(CodeBuffer cb)
			{
				// we set the RTI filter here
				cb.Append(this.fullName + "::setTypeFilter(rti.getNestedStructTypeInfo(\"" + this
					.name + "\"));\n");
			}

			internal virtual void GenSetupRTIFields(CodeBuffer cb)
			{
				cb.Append("void " + this.fullName + "::setupRtiFields() {\n");
				cb.Append("if (NULL == p" + Consts.RtiFilter + ") return;\n");
				cb.Append("if (NULL != p" + Consts.RtiFilterFields + ") return;\n");
				cb.Append("p" + Consts.RtiFilterFields + " = new int[p" + Consts.RtiFilter + "->getFieldTypeInfos().size()];\n"
					);
				cb.Append("for (unsigned int " + Consts.RioPrefix + "i=0; " + Consts.RioPrefix + 
					"i<p" + Consts.RtiFilter + "->getFieldTypeInfos().size(); " + Consts.RioPrefix +
					 "i++) {\n");
				cb.Append("p" + Consts.RtiFilterFields + "[" + Consts.RioPrefix + "i] = 0;\n");
				cb.Append("}\n");
				cb.Append("for (unsigned int " + Consts.RioPrefix + "i=0; " + Consts.RioPrefix + 
					"i<p" + Consts.RtiFilter + "->getFieldTypeInfos().size(); " + Consts.RioPrefix +
					 "i++) {\n");
				cb.Append("for (unsigned int " + Consts.RioPrefix + "j=0; " + Consts.RioPrefix + 
					"j<p" + Consts.RtiVar + "->getFieldTypeInfos().size(); " + Consts.RioPrefix + "j++) {\n"
					);
				cb.Append("if (*(p" + Consts.RtiFilter + "->getFieldTypeInfos()[" + Consts.RioPrefix
					 + "i]) == *(p" + Consts.RtiVar + "->getFieldTypeInfos()[" + Consts.RioPrefix + 
					"j])) {\n");
				cb.Append("p" + Consts.RtiFilterFields + "[" + Consts.RioPrefix + "i] = " + Consts
					.RioPrefix + "j+1;\n");
				cb.Append("break;\n");
				cb.Append("}\n");
				cb.Append("}\n");
				cb.Append("}\n");
				cb.Append("}\n");
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void GenCode(FileWriter hh, FileWriter cc, AList<string> options
				)
			{
				CodeBuffer hb = new CodeBuffer();
				string[] ns = this.module.Split("::");
				for (int i = 0; i < ns.Length; i++)
				{
					hb.Append("namespace " + ns[i] + " {\n");
				}
				hb.Append("class " + this.name + " : public ::hadoop::Record {\n");
				hb.Append("private:\n");
				for (IEnumerator<JField<JType.CppType>> i_1 = this.fields.GetEnumerator(); i_1.HasNext
					(); )
				{
					JField<JType.CppType> jf = i_1.Next();
					string name = jf.GetName();
					JType.CppType type = jf.GetType();
					type.GenDecl(hb, name);
				}
				// type info vars
				hb.Append("static ::hadoop::RecordTypeInfo* p" + Consts.RtiVar + ";\n");
				hb.Append("static ::hadoop::RecordTypeInfo* p" + Consts.RtiFilter + ";\n");
				hb.Append("static int* p" + Consts.RtiFilterFields + ";\n");
				hb.Append("static ::hadoop::RecordTypeInfo* setupTypeInfo();\n");
				hb.Append("static void setupRtiFields();\n");
				hb.Append("virtual void deserializeWithoutFilter(::hadoop::IArchive& " + Consts.RecordInput
					 + ", const char* " + Consts.Tag + ");\n");
				hb.Append("public:\n");
				hb.Append("static const ::hadoop::RecordTypeInfo& getTypeInfo() " + "{return *p" 
					+ Consts.RtiVar + ";}\n");
				hb.Append("static void setTypeFilter(const ::hadoop::RecordTypeInfo& rti);\n");
				hb.Append("static void setTypeFilter(const ::hadoop::RecordTypeInfo* prti);\n");
				hb.Append("virtual void serialize(::hadoop::OArchive& " + Consts.RecordOutput + ", const char* "
					 + Consts.Tag + ") const;\n");
				hb.Append("virtual void deserialize(::hadoop::IArchive& " + Consts.RecordInput + 
					", const char* " + Consts.Tag + ");\n");
				hb.Append("virtual const ::std::string& type() const;\n");
				hb.Append("virtual const ::std::string& signature() const;\n");
				hb.Append("virtual bool operator<(const " + this.name + "& peer_) const;\n");
				hb.Append("virtual bool operator==(const " + this.name + "& peer_) const;\n");
				hb.Append("virtual ~" + this.name + "() {};\n");
				for (IEnumerator<JField<JType.CppType>> i_2 = this.fields.GetEnumerator(); i_2.HasNext
					(); )
				{
					JField<JType.CppType> jf = i_2.Next();
					string name = jf.GetName();
					JType.CppType type = jf.GetType();
					type.GenGetSet(hb, name);
				}
				hb.Append("}; // end record " + this.name + "\n");
				for (int i_3 = ns.Length - 1; i_3 >= 0; i_3--)
				{
					hb.Append("} // end namespace " + ns[i_3] + "\n");
				}
				hh.Write(hb.ToString());
				CodeBuffer cb = new CodeBuffer();
				// initialize type info vars
				cb.Append("::hadoop::RecordTypeInfo* " + this.fullName + "::p" + Consts.RtiVar + 
					" = " + this.fullName + "::setupTypeInfo();\n");
				cb.Append("::hadoop::RecordTypeInfo* " + this.fullName + "::p" + Consts.RtiFilter
					 + " = NULL;\n");
				cb.Append("int* " + this.fullName + "::p" + Consts.RtiFilterFields + " = NULL;\n\n"
					);
				// setupTypeInfo()
				cb.Append("::hadoop::RecordTypeInfo* " + this.fullName + "::setupTypeInfo() {\n");
				cb.Append("::hadoop::RecordTypeInfo* p = new ::hadoop::RecordTypeInfo(\"" + this.
					name + "\");\n");
				for (IEnumerator<JField<JType.CppType>> i_4 = this.fields.GetEnumerator(); i_4.HasNext
					(); )
				{
					JField<JType.CppType> jf = i_4.Next();
					string name = jf.GetName();
					JType.CppType type = jf.GetType();
					type.GenStaticTypeInfo(cb, name);
				}
				cb.Append("return p;\n");
				cb.Append("}\n");
				// setTypeFilter()
				cb.Append("void " + this.fullName + "::setTypeFilter(const " + "::hadoop::RecordTypeInfo& rti) {\n"
					);
				cb.Append("if (NULL != p" + Consts.RtiFilter + ") {\n");
				cb.Append("delete p" + Consts.RtiFilter + ";\n");
				cb.Append("}\n");
				cb.Append("p" + Consts.RtiFilter + " = new ::hadoop::RecordTypeInfo(rti);\n");
				cb.Append("if (NULL != p" + Consts.RtiFilterFields + ") {\n");
				cb.Append("delete p" + Consts.RtiFilterFields + ";\n");
				cb.Append("}\n");
				cb.Append("p" + Consts.RtiFilterFields + " = NULL;\n");
				// set RTIFilter for nested structs. We may end up with multiple lines that 
				// do the same thing, if the same struct is nested in more than one field, 
				// but that's OK. 
				for (IEnumerator<JField<JType.CppType>> i_5 = this.fields.GetEnumerator(); i_5.HasNext
					(); )
				{
					JField<JType.CppType> jf = i_5.Next();
					JType.CppType type = jf.GetType();
					type.GenSetRTIFilter(cb);
				}
				cb.Append("}\n");
				// setTypeFilter()
				cb.Append("void " + this.fullName + "::setTypeFilter(const " + "::hadoop::RecordTypeInfo* prti) {\n"
					);
				cb.Append("if (NULL != prti) {\n");
				cb.Append("setTypeFilter(*prti);\n");
				cb.Append("}\n");
				cb.Append("}\n");
				// setupRtiFields()
				this.GenSetupRTIFields(cb);
				// serialize()
				cb.Append("void " + this.fullName + "::serialize(::hadoop::OArchive& " + Consts.RecordOutput
					 + ", const char* " + Consts.Tag + ") const {\n");
				cb.Append(Consts.RecordOutput + ".startRecord(*this," + Consts.Tag + ");\n");
				for (IEnumerator<JField<JType.CppType>> i_6 = this.fields.GetEnumerator(); i_6.HasNext
					(); )
				{
					JField<JType.CppType> jf = i_6.Next();
					string name = jf.GetName();
					JType.CppType type = jf.GetType();
					if (type is JBuffer.CppBuffer)
					{
						cb.Append(Consts.RecordOutput + ".serialize(" + name + "," + name + ".length(),\""
							 + name + "\");\n");
					}
					else
					{
						cb.Append(Consts.RecordOutput + ".serialize(" + name + ",\"" + name + "\");\n");
					}
				}
				cb.Append(Consts.RecordOutput + ".endRecord(*this," + Consts.Tag + ");\n");
				cb.Append("return;\n");
				cb.Append("}\n");
				// deserializeWithoutFilter()
				cb.Append("void " + this.fullName + "::deserializeWithoutFilter(::hadoop::IArchive& "
					 + Consts.RecordInput + ", const char* " + Consts.Tag + ") {\n");
				cb.Append(Consts.RecordInput + ".startRecord(*this," + Consts.Tag + ");\n");
				for (IEnumerator<JField<JType.CppType>> i_7 = this.fields.GetEnumerator(); i_7.HasNext
					(); )
				{
					JField<JType.CppType> jf = i_7.Next();
					string name = jf.GetName();
					JType.CppType type = jf.GetType();
					if (type is JBuffer.CppBuffer)
					{
						cb.Append("{\nsize_t len=0; " + Consts.RecordInput + ".deserialize(" + name + ",len,\""
							 + name + "\");\n}\n");
					}
					else
					{
						cb.Append(Consts.RecordInput + ".deserialize(" + name + ",\"" + name + "\");\n");
					}
				}
				cb.Append(Consts.RecordInput + ".endRecord(*this," + Consts.Tag + ");\n");
				cb.Append("return;\n");
				cb.Append("}\n");
				// deserialize()
				cb.Append("void " + this.fullName + "::deserialize(::hadoop::IArchive& " + Consts
					.RecordInput + ", const char* " + Consts.Tag + ") {\n");
				cb.Append("if (NULL == p" + Consts.RtiFilter + ") {\n");
				cb.Append("deserializeWithoutFilter(" + Consts.RecordInput + ", " + Consts.Tag + 
					");\n");
				cb.Append("return;\n");
				cb.Append("}\n");
				cb.Append("// if we're here, we need to read based on version info\n");
				cb.Append(Consts.RecordInput + ".startRecord(*this," + Consts.Tag + ");\n");
				cb.Append("setupRtiFields();\n");
				cb.Append("for (unsigned int " + Consts.RioPrefix + "i=0; " + Consts.RioPrefix + 
					"i<p" + Consts.RtiFilter + "->getFieldTypeInfos().size(); " + Consts.RioPrefix +
					 "i++) {\n");
				int ct = 0;
				for (IEnumerator<JField<JType.CppType>> i_8 = this.fields.GetEnumerator(); i_8.HasNext
					(); )
				{
					JField<JType.CppType> jf = i_8.Next();
					string name = jf.GetName();
					JType.CppType type = jf.GetType();
					ct++;
					if (1 != ct)
					{
						cb.Append("else ");
					}
					cb.Append("if (" + ct + " == p" + Consts.RtiFilterFields + "[" + Consts.RioPrefix
						 + "i]) {\n");
					if (type is JBuffer.CppBuffer)
					{
						cb.Append("{\nsize_t len=0; " + Consts.RecordInput + ".deserialize(" + name + ",len,\""
							 + name + "\");\n}\n");
					}
					else
					{
						cb.Append(Consts.RecordInput + ".deserialize(" + name + ",\"" + name + "\");\n");
					}
					cb.Append("}\n");
				}
				if (0 != ct)
				{
					cb.Append("else {\n");
					cb.Append("const std::vector< ::hadoop::FieldTypeInfo* >& typeInfos = p" + Consts
						.RtiFilter + "->getFieldTypeInfos();\n");
					cb.Append("::hadoop::Utils::skip(" + Consts.RecordInput + ", typeInfos[" + Consts
						.RioPrefix + "i]->getFieldID()->c_str()" + ", *(typeInfos[" + Consts.RioPrefix +
						 "i]->getTypeID()));\n");
					cb.Append("}\n");
				}
				cb.Append("}\n");
				cb.Append(Consts.RecordInput + ".endRecord(*this, " + Consts.Tag + ");\n");
				cb.Append("}\n");
				// operator <
				cb.Append("bool " + this.fullName + "::operator< (const " + this.fullName + "& peer_) const {\n"
					);
				cb.Append("return (1\n");
				for (IEnumerator<JField<JType.CppType>> i_9 = this.fields.GetEnumerator(); i_9.HasNext
					(); )
				{
					JField<JType.CppType> jf = i_9.Next();
					string name = jf.GetName();
					cb.Append("&& (" + name + " < peer_." + name + ")\n");
				}
				cb.Append(");\n");
				cb.Append("}\n");
				cb.Append("bool " + this.fullName + "::operator== (const " + this.fullName + "& peer_) const {\n"
					);
				cb.Append("return (1\n");
				for (IEnumerator<JField<JType.CppType>> i_10 = this.fields.GetEnumerator(); i_10.
					HasNext(); )
				{
					JField<JType.CppType> jf = i_10.Next();
					string name = jf.GetName();
					cb.Append("&& (" + name + " == peer_." + name + ")\n");
				}
				cb.Append(");\n");
				cb.Append("}\n");
				cb.Append("const ::std::string&" + this.fullName + "::type() const {\n");
				cb.Append("static const ::std::string type_(\"" + this.name + "\");\n");
				cb.Append("return type_;\n");
				cb.Append("}\n");
				cb.Append("const ::std::string&" + this.fullName + "::signature() const {\n");
				cb.Append("static const ::std::string sig_(\"" + this._enclosing.GetSignature() +
					 "\");\n");
				cb.Append("return sig_;\n");
				cb.Append("}\n");
				cc.Write(cb.ToString());
			}

			private readonly JRecord _enclosing;
		}

		internal class CRecord : JCompType.CCompType
		{
			internal CRecord(JRecord _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JRecord _enclosing;
		}

		private string signature;

		/// <summary>Creates a new instance of JRecord</summary>
		public JRecord(string name, AList<JField<JType>> flist)
		{
			SetJavaType(new JRecord.JavaRecord(this, name, flist));
			SetCppType(new JRecord.CppRecord(this, name, flist));
			SetCType(new JRecord.CRecord(this));
			// precompute signature
			int idx = name.LastIndexOf('.');
			string recName = Sharpen.Runtime.Substring(name, idx + 1);
			StringBuilder sb = new StringBuilder();
			sb.Append("L").Append(recName).Append("(");
			for (IEnumerator<JField<JType>> i = flist.GetEnumerator(); i.HasNext(); )
			{
				string s = i.Next().GetType().GetSignature();
				sb.Append(s);
			}
			sb.Append(")");
			signature = sb.ToString();
		}

		internal override string GetSignature()
		{
			return signature;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void GenCppCode(FileWriter hh, FileWriter cc, AList<string> options
			)
		{
			((JRecord.CppRecord)GetCppType()).GenCode(hh, cc, options);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void GenJavaCode(string destDir, AList<string> options)
		{
			((JRecord.JavaRecord)GetJavaType()).GenCode(destDir, options);
		}
	}
}
