r'''Make i2b2 metadata terms

see dmtoterms.rst for design notes and tests.
'''

__author__ = 'Dan Connolly'
__contact__ = 'http://informatics.kumc.edu/'
__copyright__ = 'Copyright (c) 2014 University of Kansas Medical Center'
__license__ = 'MIT'
__docformat__ = "restructuredtext en"


from collections import namedtuple


Term = namedtuple('Term',
                  ['c_hlevel',
                   'c_fullname', 'c_basecode', 'c_name',
                   'c_visualattributes',
                   'm_applied_path', 'c_tooltip',
                   'c_synonym_cd',
                   'update_date', 'sourcesystem_cd',
                   'c_metadataxml',
                   'c_dimcode', 'c_operator', 'c_columndatatype',
                   'c_columnname', 'c_tablename', 'c_facttablecolumn'])


class I2B2MetaData(object):
    @classmethod
    def term(cls, pfx, parts, name,
             code=None, viz='CAE', applies_to=[], tooltip=None,
             c_synonym_cd='N',
             update_date=None,  # audit info can be added later
             sourcesystem_cd=None,
             c_metadataxml=None,
             c_operator='like',
             c_columndatatype='@',
             c_columnname='concept_path',
             c_tablename='concept_dimension',
             c_facttablecolumn='concept_cd',
             max_tooltip_len=850, encoding='utf-8'):
        '''

        Where there is no tooltip, copy the name into the tooltip
        field so the web client doesn't show null:

        >>> I2B2MetaData.term(['ROOT'], ['T'], 'name').c_tooltip
        'name'

        If there's a double quote in the tooltip, the web client
        gets confused and doesn't display the tooltip.
        KLUDGE: change them to single quotes.

        >>> I2B2MetaData.term(['ROOT'], ['T'], 'T',
        ...                   tooltip='ab "x"').c_tooltip
        "ab 'x'"

        '''

        # TODO: move pfx to caller's responsibility
        # TODO: postpone serialization?
        hlevel, path = len(parts), '\\'.join(pfx + parts + [''])
        m_path = '\\'.join(applies_to + ['%']) if applies_to else '@'

        tooltip_enc = (tooltip or name)[:max_tooltip_len].encode(encoding)
        tooltip_q = tooltip_enc.replace('"', "'")

        return Term(
            hlevel, path,
            code, name, viz,
            m_path, tooltip_q,
            c_synonym_cd,
            update_date, sourcesystem_cd,
            c_metadataxml or '',
            path, c_operator, c_columndatatype, c_columnname,
            c_tablename, c_facttablecolumn)

    @classmethod
    def top_term(cls, root):
        return cls.term(parts=[], pfx=['', root], name=root)

    @classmethod
    def folders(cls, pfx, path_parts, names, tooltips, vizs,
                mapp=None):
        return [cls.term(pfx=pfx,
                         parts=parts,
                         applies_to=app,
                         name=n,
                         viz=viz,
                         tooltip=tooltip)
                for (parts, n, viz, tooltip, app)
                in zip(path_parts, names, vizs, tooltips,
                       mapp or [[] for _ in path_parts])]

    @classmethod
    def discrete_terms(cls, pfx,
                       path_parts, codes, applies, names, vizs):
        return [
            cls.term(pfx=pfx,
                     parts=parts,
                     code=code,
                     applies_to=applies_to,
                     name=name,
                     viz=viz)
            for (parts, code, applies_to, name, viz)
            in zip(path_parts, codes, applies, names, vizs)]

    @classmethod
    def scalars(cls, pfx, field_parts, codes,
                names, value_info, vizs, tooltips):
        return [cls.term(pfx=pfx,
                         parts=parts, code=code, name=name,
                         c_columndatatype=ct,
                         c_metadataxml=cls.metadataxml(
                             datatype=datatype,
                             normalunits=units),
                         viz=viz,
                         tooltip=tooltip)
                for (parts, code, name, (ct, datatype, units), viz, tooltip)
                in zip(field_parts, codes, names, value_info, vizs, tooltips)]

    @classmethod
    def metadataxml(cls, normalunits,
                    datatype='Float', oktousevalues='Y',
                    # arbitrary
                    creation='01/26/2011 00:00:00'):
        '''
        ref: `XML Specification for Descriptive Files of Values`__

        __ https://community.i2b2.org/wiki/display/DevForum/Metadata+XML+for+Medication+Modifiers  # noqa
        '''

        # TODO? <MaxStringLength />
        #  <LowofLowValue />
        #  <HighofLowValue />
        #  <LowofHighValue />
        #  <HighofHighValue />
        #  <LowofToxicValue />
        #  <HighofToxicValue />
        #  <ConvertingUnits>
        #    <Units>cm</Units>
        #    <MultiplyingFactor>0.393701</MultiplyingFactor>
        #  </ConvertingUnits>
        return '''
        <ValueMetadata>
          <Version>3.2</Version>
          <CreationDateTime>%(CREATION)s</CreationDateTime>
          <DataType>%(DATATYPE)s</DataType>
          <Oktousevalues>%(OKTOUSEVALUES)s</Oktousevalues>
          <UnitValues>
            <NormalUnits>%(NORMALUNITS)s</NormalUnits>
          </UnitValues>
        </ValueMetadata>
        ''' % dict(CREATION=creation, DATATYPE=datatype,
                   OKTOUSEVALUES=oktousevalues,
                   NORMALUNITS=normalunits)
