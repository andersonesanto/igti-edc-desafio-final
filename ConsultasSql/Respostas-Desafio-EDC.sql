/* 1) Quantos alunos NÃO quiseram declarar a cor/raça em 2019 (Entenda que o aluno marcou a opção de “não quero declarar” nessa pergunta)? */
SELECT count(*) tp_cor_raca
FROM aluno
WHERE tp_cor_raca = 0;
/* resposta 2227513 */
/* 2) Qual é o número de alunos do Sexo Feminino que nasceram no estado de código 35? */
SELECT count(*) alunos_sexo_feminino
FROM  aluno
WHERE tp_sexo = 1
	AND co_uf_nascimento = 35;
/*Resposta 1004194*/
/* 3) Quantos alunos do sexo feminino no estado de código 43 ingressaram no ensino superior por vagas de cotas étnicas?*/
SELECT count(*) alunos_sexo_feminino
FROM aluno
WHERE tp_sexo = 1
	AND co_uf_nascimento = 43
	and ;