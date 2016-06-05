using System.Collections.Generic;


namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>Abstract Base class for all types supported by Hadoop Record I/O.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public abstract class JType
	{
		internal static string ToCamelCase(string name)
		{
			char firstChar = name[0];
			if (System.Char.IsLower(firstChar))
			{
				return string.Empty + System.Char.ToUpper(firstChar) + Runtime.Substring(
					name, 1);
			}
			return name;
		}

		internal JType.JavaType javaType;

		internal JType.CppType cppType;

		internal JType.CType cType;

		internal abstract class JavaType
		{
			private string name;

			private string methodSuffix;

			private string wrapper;

			private string typeIDByteString;

			internal JavaType(JType _enclosing, string javaname, string suffix, string wrapper
				, string typeIDByteString)
			{
				this._enclosing = _enclosing;
				// points to TypeID.RIOType 
				this.name = javaname;
				this.methodSuffix = suffix;
				this.wrapper = wrapper;
				this.typeIDByteString = typeIDByteString;
			}

			internal virtual void GenDecl(CodeBuffer cb, string fname)
			{
				cb.Append("private " + this.name + " " + fname + ";\n");
			}

			internal virtual void GenStaticTypeInfo(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RtiVar + ".addField(\"" + fname + "\", " + this.GetTypeIDObjectString
					() + ");\n");
			}

			internal abstract string GetTypeIDObjectString();

			internal virtual void GenSetRTIFilter(CodeBuffer cb, IDictionary<string, int> nestedStructMap
				)
			{
				// do nothing by default
				return;
			}

			/*void genRtiFieldCondition(CodeBuffer cb, String fname, int ct) {
			cb.append("if ((tInfo.fieldID.equals(\"" + fname + "\")) && (typeVal ==" +
			" org.apache.hadoop.record.meta." + getTypeIDByteString() + ")) {\n");
			cb.append("rtiFilterFields[i] = " + ct + ";\n");
			cb.append("}\n");
			}
			
			void genRtiNestedFieldCondition(CodeBuffer cb, String varName, int ct) {
			cb.append("if (" + varName + ".getElementTypeID().getTypeVal() == " +
			"org.apache.hadoop.record.meta." + getTypeIDByteString() +
			") {\n");
			cb.append("rtiFilterFields[i] = " + ct + ";\n");
			cb.append("}\n");
			}*/
			internal virtual void GenConstructorParam(CodeBuffer cb, string fname)
			{
				cb.Append("final " + this.name + " " + fname);
			}

			internal virtual void GenGetSet(CodeBuffer cb, string fname)
			{
				cb.Append("public " + this.name + " get" + JType.ToCamelCase(fname) + "() {\n");
				cb.Append("return " + fname + ";\n");
				cb.Append("}\n");
				cb.Append("public void set" + JType.ToCamelCase(fname) + "(final " + this.name + 
					" " + fname + ") {\n");
				cb.Append("this." + fname + "=" + fname + ";\n");
				cb.Append("}\n");
			}

			internal virtual string GetType()
			{
				return this.name;
			}

			internal virtual string GetWrapperType()
			{
				return this.wrapper;
			}

			internal virtual string GetMethodSuffix()
			{
				return this.methodSuffix;
			}

			internal virtual string GetTypeIDByteString()
			{
				return this.typeIDByteString;
			}

			internal virtual void GenWriteMethod(CodeBuffer cb, string fname, string tag)
			{
				cb.Append(Consts.RecordOutput + ".write" + this.methodSuffix + "(" + fname + ",\""
					 + tag + "\");\n");
			}

			internal virtual void GenReadMethod(CodeBuffer cb, string fname, string tag, bool
				 decl)
			{
				if (decl)
				{
					cb.Append(this.name + " " + fname + ";\n");
				}
				cb.Append(fname + "=" + Consts.RecordInput + ".read" + this.methodSuffix + "(\"" 
					+ tag + "\");\n");
			}

			internal virtual void GenCompareTo(CodeBuffer cb, string fname, string other)
			{
				cb.Append(Consts.RioPrefix + "ret = (" + fname + " == " + other + ")? 0 :((" + fname
					 + "<" + other + ")?-1:1);\n");
			}

			internal abstract void GenCompareBytes(CodeBuffer cb);

			internal abstract void GenSlurpBytes(CodeBuffer cb, string b, string s, string l);

			internal virtual void GenEquals(CodeBuffer cb, string fname, string peer)
			{
				cb.Append(Consts.RioPrefix + "ret = (" + fname + "==" + peer + ");\n");
			}

			internal virtual void GenHashCode(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RioPrefix + "ret = (int)" + fname + ";\n");
			}

			internal virtual void GenConstructorSet(CodeBuffer cb, string fname)
			{
				cb.Append("this." + fname + " = " + fname + ";\n");
			}

			internal virtual void GenClone(CodeBuffer cb, string fname)
			{
				cb.Append(Consts.RioPrefix + "other." + fname + " = this." + fname + ";\n");
			}

			private readonly JType _enclosing;
		}

		internal abstract class CppType
		{
			private string name;

			internal CppType(JType _enclosing, string cppname)
			{
				this._enclosing = _enclosing;
				this.name = cppname;
			}

			internal virtual void GenDecl(CodeBuffer cb, string fname)
			{
				cb.Append(this.name + " " + fname + ";\n");
			}

			internal virtual void GenStaticTypeInfo(CodeBuffer cb, string fname)
			{
				cb.Append("p->addField(new ::std::string(\"" + fname + "\"), " + this.GetTypeIDObjectString
					() + ");\n");
			}

			internal virtual void GenGetSet(CodeBuffer cb, string fname)
			{
				cb.Append("virtual " + this.name + " get" + JType.ToCamelCase(fname) + "() const {\n"
					);
				cb.Append("return " + fname + ";\n");
				cb.Append("}\n");
				cb.Append("virtual void set" + JType.ToCamelCase(fname) + "(" + this.name + " m_) {\n"
					);
				cb.Append(fname + "=m_;\n");
				cb.Append("}\n");
			}

			internal abstract string GetTypeIDObjectString();

			internal virtual void GenSetRTIFilter(CodeBuffer cb)
			{
				// do nothing by default
				return;
			}

			internal virtual string GetType()
			{
				return this.name;
			}

			private readonly JType _enclosing;
		}

		internal class CType
		{
			internal CType(JType _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JType _enclosing;
		}

		internal abstract string GetSignature();

		internal virtual void SetJavaType(JType.JavaType jType)
		{
			this.javaType = jType;
		}

		internal virtual JType.JavaType GetJavaType()
		{
			return javaType;
		}

		internal virtual void SetCppType(JType.CppType cppType)
		{
			this.cppType = cppType;
		}

		internal virtual JType.CppType GetCppType()
		{
			return cppType;
		}

		internal virtual void SetCType(JType.CType cType)
		{
			this.cType = cType;
		}

		internal virtual JType.CType GetCType()
		{
			return cType;
		}
	}
}
