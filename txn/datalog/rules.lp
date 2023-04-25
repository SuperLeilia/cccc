% Inference rules
cc(X, Y) :- so(X, Y) .
cc(X, Y) :- wr(X, Y, K) .
cc(X, Z) :- cc(X, Y), cc(Y, Z) .
co(X, Y) :- cc(X, Z), wr(Y, Z, K), wtxn(X, K), wsv(X, Y) .
tcc(X, Y) :- cc(X, Y) .
tcc(X, Y) :- co(X, Y) .
tcc(X, Z) :- tcc(X ,Y), tcc(Y, Z) .

% Cycle
% show(X, Y) :- tcc(X ,Y), tcc(Y, X) .
bad_cyclicCO(X) :- tcc(X, X) .  % 这个对应于带有co的cycle
% bad_cyclicSOWR(X) :- cc(X, X) .  % 这个对应于oopsla里soUwr的cycle；或许具体试验时候不需要这条

