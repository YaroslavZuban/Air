select floor(exp(sum(ln(PassengerId))))
from default.titanic 
where PassengerId < 10
