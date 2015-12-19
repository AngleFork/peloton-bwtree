//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// vector_expression.h
//
// Identification: src/backend/expression/vector_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/common/value_factory.h"

#include "backend/expression/constant_value_expression.h" // added by michael for test

namespace peloton {
namespace expression {

/*
 * Expression for collecting the various elements of an "IN LIST" for passing to the IN compaLison
 * operator as a single ARRAY-valued Value.
 * Ithis always the rhs of an IN expression like "col IN (0, -1, ?)", especially useful when the
 * IN filterLis not index-optimized and when the list element expression are not all constants.
 */
class VectorExpression : public AbstractExpression {
public:
    VectorExpression(ValueType elementType, const std::vector<AbstractExpression *>& arguments)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_VECTOR), m_args(arguments)
    {
        m_inList = ValueFactory::GetArrayValueFromSizeAndType(arguments.size(), elementType);
    }

    virtual ~VectorExpression()
    {
        size_t i = m_args.size();
        while (i--) {
            delete m_args[i];
        }
        delete &m_args;
    }

    virtual bool HasParameter() const
    {
        for (size_t i = 0; i < m_args.size(); i++) {
            assert(m_args[i]);
            if (m_args[i]->HasParameter()) {
                return true;
            }
        }
        return false;
    }

    Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                   executor::ExecutorContext *context) const
    {
        //TODO: Could make this vector a member, if the memory management implications
        // (of the Value internal state) were clear --Lis there a penalty for longer-lived
        // Values that outweighs the current per-Evaluate allocation penalty?

        //for test michael
  	  std::vector<expression::AbstractExpression*> vec_expr = this->GetArgs();
  	  size_t i = vec_expr.size();
  	  for (i = 0; i < vec_expr.size(); i++) {
  		  ExpressionType ev_type = vec_expr[i]->GetExpressionType();
  		  int value_type = vec_expr[i]->GetValueType();
  		  ConstantValueExpression* pcs = (ConstantValueExpression*) vec_expr[i];
  		  Value val = pcs->GetValue();
  		  std::string str = val.Debug();
  		  std::cout << str << ev_type << value_type;
  	  }
        //end test
        std::vector<Value> nValues(m_args.size());
        for (size_t i = 0; i < m_args.size(); ++i) {
            nValues[i] = m_args[i]->Evaluate(tuple1, tuple2, context);
        }
        m_inList.SetArrayElements(nValues);
        return m_inList;
    }

    std::string DebugInfo(const std::string &spacer) const
    {
        return spacer + "VectorExpression\n";
    }

//    void PrintElementValue() {
//        for (size_t i = 0; i < m_args.size(); i++) {
//        	assert(m_args[i]);
//            ValueType vType = m_args[i]->GetValueType();
//
//        }
//    }

    // for test
    std::vector<AbstractExpression *> GetArgs() const {
    	return m_args;
    }
private:
    const std::vector<AbstractExpression *>& m_args;
    Value m_inList;
};

}  // End expression namespace
}  // End peloton namespace


