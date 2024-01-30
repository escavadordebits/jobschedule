select  T0.DocEntry DocAdto,T1.DocEntry,T0.DocTotal DocNota from DRAFTS T1
inner join INVOICES T0 on T0.CardCode=T1.CardCode
WHERE
t1.DocTotal = t0.DocTotal
and t1.U_DwnPmtAuto = 'S'