co(X,Y) :- po(X,Y).
co(X,Y) :- wr(X,Y).
co(X,Z) :- co(X,Y), co(Y,Z).
reach(X,Y) :- co(X,Y).
reach(Z,Y) :- co(X,Y), wr(X,Z), wrt(Y), sv(Y,Z).
reach(X,Z) :- reach(X,Y), reach(Y,Z).
bad_cyclicCO(X) :- reach(X,X).
bad_writeCOInitRead(X,Y):- co(X,Y), wrt(X), initread(Y), sv(X,Y).